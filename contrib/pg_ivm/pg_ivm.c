/*-------------------------------------------------------------------------
 *
 * pg_ivm.c
 *	  incremental view maintenance extension
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 2022, IVM Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_trigger_d.h"
#include "commands/createas.h"
#include "commands/trigger.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/print.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
//#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

#define Natts_pg_ivm_immv 2

#define Anum_pg_ivm_immv_immvrelid 1
#define Anum_pg_ivm_immv_viewdef 2

PG_MODULE_MAGIC;

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	ObjectAddress reladdr;		/* address of rel, for ExecCreateTableAs */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_intorel;

static void CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock);
static void CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock);
static void check_ivm_restriction(Node *node);

static Oid get_immv_catalog(void);

void CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid, bool is_create);
void CreateIndexOnIMMV(Query *query, Relation matviewRel, bool is_create);
Query *rewriteQueryForIMMV(Query *query, List *colNames);

void		_PG_init(void);

PG_FUNCTION_INFO_V1(create_immv);

Datum
create_immv(PG_FUNCTION_ARGS)
{
	text       *t_relname = PG_GETARG_TEXT_PP(0);
	text       *t_sql = PG_GETARG_TEXT_PP(1);
	char *relname;
	char *sql;
	List *parsetree_list;
	RawStmt    *parsetree;

	ParseState *pstate = make_parsestate(NULL);
	Query *query;
	Query *viewQuery;
	IntoClause *into;
	CreateTableAsStmt *stmt;
	StringInfoData command_buf;

	//bool		is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;

	//pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	sql = text_to_cstring(t_sql);
	relname = text_to_cstring(t_relname);

	initStringInfo(&command_buf);
	appendStringInfo(&command_buf, "CREATE MATERIALIZED VIEW %s AS %s;", relname, sql);
	parsetree_list = pg_parse_query(command_buf.data);
	pstate->p_sourcetext = command_buf.data;

	/* XXX: should we check t_sql before command_buf? */
	if (list_length(parsetree_list) != 1)
		elog(ERROR, "invalid view definition");

	parsetree = linitial_node(RawStmt, parsetree_list);
	query = transformStmt(pstate, parsetree->stmt);

	Assert(query->commandType == CMD_UTILITY && IsA(query->utilityStmt, CreateTableAsStmt));

	stmt = (CreateTableAsStmt*) query->utilityStmt;
	into = stmt->into;
	query = castNode(Query, stmt->query);

	/* Check if the relation exists or not */
	if (CreateTableAsRelExists(stmt))
		elog(ERROR, "error");

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	viewQuery = (Query *) into->viewQuery;
	into->viewQuery = NULL;
	dest = CreateIntoRelDestReceiver(into);

	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	//if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	//if (is_matview && into->ivm)
	{
		/* check if the query is supported in IMMV definition */
		if (contain_mutable_functions((Node *) query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mutable function is not supported on incrementally maintainable materialized view"),
					 errhint("functions must be marked IMMUTABLE")));

		// TODO:
		//check_ivm_restriction((Node *) query);

		/* For IMMV, we need to rewrite matview query */
		query = rewriteQueryForIMMV(query, into->colNames);
	}

	//if (into->skipData)
	//{
		/*
		 * If WITH NO DATA was specified, do not go through the rewriter,
		 * planner and executor.  Just define the relation using a code path
		 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
		 * from running the planner before all dependencies are set up.
		 */
		//address = create_ctas_nodata(query->targetList, into);
	//}
	//else
	{
		/*
		 * Parse analysis was done already, but we still have to run the rule
		 * rewriter.  We do not do AcquireRewriteLocks: we assume the query
		 * either came straight from the parser, or suitable locks were
		 * acquired by plancache.c.
		 */
		rewritten = QueryRewrite(query);

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for CREATE MATERIALIZED VIEW");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plan = pg_plan_query(query, pstate->p_sourcetext,
							 CURSOR_OPT_PARALLEL_OK, NULL);

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.  (This could only
		 * matter if the planner executed an allegedly-stable function that
		 * changed the database contents, but let's do it anyway to be
		 * parallel to the EXPLAIN code path.)
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create a QueryDesc, redirecting output to our tuple receiver */
		queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
									GetActiveSnapshot(), InvalidSnapshot,
									dest, NULL, NULL, 0);

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* save the rowcount if we're given a qc to fill */
		//if (qc)
			//SetQueryCompletion(qc, CMDTAG_SELECT, queryDesc->estate->es_processed);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		/* and clean up */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		FreeQueryDesc(queryDesc);

		PopActiveSnapshot();
	}

	/* Create the "view" part of a materialized view. */
	//if (is_matview)
	{
		//StoreViewQuery(address.objectId, (Query*) viewQuery, false);
		char   *querytree = nodeToString((Node *) viewQuery);
		Datum values[Natts_pg_ivm_immv];
		bool isNulls[Natts_pg_ivm_immv];
		Relation pgIvmImmv;
    	TupleDesc tupleDescriptor;
    	HeapTuple heapTuple;

		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		values[Anum_pg_ivm_immv_immvrelid -1 ] = ObjectIdGetDatum(address.objectId);
		values[Anum_pg_ivm_immv_viewdef -1 ] = CStringGetTextDatum(querytree);

		pgIvmImmv = table_open(get_immv_catalog(), RowExclusiveLock);

    	tupleDescriptor = RelationGetDescr(pgIvmImmv);
    	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

    	CatalogTupleInsert(pgIvmImmv, heapTuple);

		table_close(pgIvmImmv, NoLock);

		CommandCounterIncrement();
	}


	//if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		//if (into->ivm)
		{
			Oid matviewOid = address.objectId;
			Relation matviewRel = table_open(matviewOid, NoLock);

			/*
			 * Mark relisivm field, if it's a matview and into->ivm is true.
			 */
			//SetMatViewIVMState(matviewRel, true);

			//if (!into->skipData)
			{
				/* Create an index on incremental maintainable materialized view, if possible */
				//CreateIndexOnIMMV((Query *) into->viewQuery, matviewRel, true);

				/* Create triggers on incremental maintainable materialized view */
				CreateIvmTriggersOnBaseTables(viewQuery, matviewOid, true);
			}
			table_close(matviewRel, NoLock);
		}
	}

	//return address;

	PG_RETURN_VOID();
}

/*
 * rewriteQueryForIMMV -- rewrite view definition query for IMMV
 *
 * count(*) is added for counting distinct tuples in views.
 */
Query *
rewriteQueryForIMMV(Query *query, List *colNames)
{
	Query *rewritten;

	TargetEntry *tle;
	Node *node;
	ParseState *pstate = make_parsestate(NULL);
	FuncCall *fn;

	rewritten = copyObject(query);
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * Convert DISTINCT to GROUP BY and add count(*) for counting distinct
	 * tuples in views.
	 */
	if (rewritten->distinctClause)
	{
		rewritten->groupClause = transformDistinctClause(NULL, &rewritten->targetList, rewritten->sortClause, false);

		fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
		fn->agg_star = true;

		node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

		tle = makeTargetEntry((Expr *) node,
								list_length(rewritten->targetList) + 1,
								pstrdup("__ivm_count__"),
								false);
		rewritten->targetList = lappend(rewritten->targetList, tle);
		rewritten->hasAggs = true;
	}

	return rewritten;
}

static Oid
get_immv_catalog(void)
{
	static Oid oid = InvalidOid;

	if (!OidIsValid(oid))
		oid = get_relname_relid("pg_ivm_immv", PG_CATALOG_NAMESPACE);

	return oid;
}

/*
 * CreateIvmTriggersOnBaseTables -- create IVM triggers on all base tables
 */
void
CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid, bool is_create)
{
	Relids	relids = NULL;
	bool	ex_lock = false;
	Index	first_rtindex = is_create ? 1 : PRS2_NEW_VARNO + 1;
	RangeTblEntry *rte;

	/* Immediately return if we don't have any base tables. */
	if (list_length(qry->rtable) < first_rtindex)
		return;

	/*
	 * If the view has more than one base tables, we need an exclusive lock
	 * on the view so that the view would be maintained serially to avoid
	 * the inconsistency that occurs when two base tables are modified in
	 * concurrent transactions. However, if the view has only one table,
	 * we can use a weaker lock.
	 *
	 * The type of lock should be determined here, because if we check the
	 * view definition at maintenance time, we need to acquire a weaker lock,
	 * and upgrading the lock level after this increases probability of
	 * deadlock.
	 */

	rte = list_nth(qry->rtable, first_rtindex - 1);
	if (list_length(qry->rtable) > first_rtindex ||
		rte->rtekind != RTE_RELATION)
		ex_lock = true;

	CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)qry, matviewOid, &relids, ex_lock);

	bms_free(relids);
}

static void
CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock)
{
	if (node == NULL)
		return;

	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *query = (Query *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)query->jointree, matviewOid, relids, ex_lock);
			}
			break;

		case T_RangeTblRef:
			{
				int			rti = ((RangeTblRef *) node)->rtindex;
				RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

				if (rte->rtekind == RTE_RELATION && !bms_is_member(rte->relid, *relids))
				{
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER, ex_lock);

					*relids = bms_add_member(*relids, rte->relid);
				}
			}
			break;

		case T_FromExpr:
			{
				FromExpr   *f = (FromExpr *) node;
				ListCell   *l;

				foreach(l, f->fromlist)
					CreateIvmTriggersOnBaseTablesRecurse(qry, lfirst(l), matviewOid, relids, ex_lock);
			}
			break;

		case T_JoinExpr:
			{
				JoinExpr   *j = (JoinExpr *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, j->larg, matviewOid, relids, ex_lock);
				CreateIvmTriggersOnBaseTablesRecurse(qry, j->rarg, matviewOid, relids, ex_lock);
			}
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
	}
}

/*
 * CreateIvmTrigger -- create IVM trigger on a base table
 */
static void
CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock)
{
	ObjectAddress	refaddr;
	ObjectAddress	address;
	CreateTrigStmt *ivm_trigger;
	List *transitionRels = NIL;

	Assert(timing == TRIGGER_TYPE_BEFORE || timing == TRIGGER_TYPE_AFTER);

	refaddr.classId = RelationRelationId;
	refaddr.objectId = viewOid;
	refaddr.objectSubId = 0;

	ivm_trigger = makeNode(CreateTrigStmt);
	ivm_trigger->relation = NULL;
	ivm_trigger->row = false;

	ivm_trigger->timing = timing;
	ivm_trigger->events = type;

	switch (type)
	{
		case TRIGGER_TYPE_INSERT:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_ins_before" : "IVM_trigger_ins_after");
			break;
		case TRIGGER_TYPE_DELETE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_del_before" : "IVM_trigger_del_after");
			break;
		case TRIGGER_TYPE_UPDATE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_upd_before" : "IVM_trigger_upd_after");
			break;
		default:
			elog(ERROR, "unsupported trigger type");
	}

	if (timing == TRIGGER_TYPE_AFTER)
	{
		if (type == TRIGGER_TYPE_INSERT || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "__ivm_newtable";
			n->isNew = true;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
		if (type == TRIGGER_TYPE_DELETE || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "__ivm_oldtable";
			n->isNew = false;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
	}

	ivm_trigger->funcname =
		(timing == TRIGGER_TYPE_BEFORE ? SystemFuncName("IVM_immediate_before") : SystemFuncName("IVM_immediate_maintenance"));

	ivm_trigger->columns = NIL;
	ivm_trigger->transitionRels = transitionRels;
	ivm_trigger->whenClause = NULL;
	ivm_trigger->isconstraint = false;
	ivm_trigger->deferrable = false;
	ivm_trigger->initdeferred = false;
	ivm_trigger->constrrel = NULL;
	ivm_trigger->args = list_make2(
		makeString(DatumGetPointer(DirectFunctionCall1(oidout, ObjectIdGetDatum(viewOid)))),
		makeString(DatumGetPointer(DirectFunctionCall1(boolout, BoolGetDatum(ex_lock))))
		);

	address = CreateTrigger(ivm_trigger, NULL, relOid, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, InvalidOid, NULL, true, false);

	// XXX: which deptype is correct??
	//recordDependencyOn(&address, &refaddr, DEPENDENCY_IMMV);
	recordDependencyOn(&address, &refaddr, DEPENDENCY_AUTO);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

PG_FUNCTION_INFO_V1(IVM_immediate_before);

Datum
IVM_immediate_before(PG_FUNCTION_ARGS)
{
	elog(INFO, "do something before modification");
	return PointerGetDatum(NULL);
}

PG_FUNCTION_INFO_V1(IVM_immediate_maintenance);

Datum
IVM_immediate_maintenance(PG_FUNCTION_ARGS)
{
	elog(INFO, "do something after modification");
	return PointerGetDatum(NULL);
}

void
_PG_init(void)
{
}

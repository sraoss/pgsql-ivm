/*-------------------------------------------------------------------------
 *
 * createas.c
 *	  Execution of CREATE TABLE ... AS, a/k/a SELECT INTO.
 *	  Since CREATE MATERIALIZED VIEW shares syntax and most behaviors,
 *	  we implement that here, too.
 *
 * We implement this by diverting the query's normal output to a
 * specialized DestReceiver type.
 *
 * Formerly, CTAS was implemented as a variant of SELECT, which led
 * to assorted legacy behaviors that we still try to preserve, notably that
 * we must return a tuples-processed count in the QueryCompletion.  (We no
 * longer do that for CTAS ... WITH NO DATA, however.)
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/createas.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"




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

typedef struct
{
	bool	has_outerjoin;
	bool	has_subquery;
	bool	has_agg;
	List	*join_quals;
} check_ivm_restriction_context;

/* utility functions for CTAS definition creation */
static ObjectAddress create_ctas_internal(List *attrList, IntoClause *into);
static ObjectAddress create_ctas_nodata(List *tlist, IntoClause *into);

/* DestReceiver routines for collecting data */
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown(DestReceiver *self);
static void intorel_destroy(DestReceiver *self);

static void CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing);
static void check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *ctx, int depth);
static bool is_equijoin_condition(OpExpr *op);
static bool check_aggregate_supports_ivm(Oid aggfnoid);

/*
 * create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
create_ctas_internal(List *attrList, IntoClause *into)
{
	CreateStmt *create = makeNode(CreateStmt);
	bool		is_matview;
	char		relkind;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress intoRelationAddr;

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create->relation = into->rel;
	create->tableElts = attrList;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = into->options;
	create->oncommit = into->onCommit;
	create->tablespacename = into->tableSpaceName;
	create->if_not_exists = false;
	/* Using Materialized view only */
	create->ivm = into->ivm;
	create->accessMethod = into->accessMethod;

	/*
	 * Create the relation.  (This will error out if there's an existing view,
	 * so we don't need more code to complain if "replace" is false.)
	 */
	intoRelationAddr = DefineRelation(create, relkind, InvalidOid, NULL, NULL);

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0,
										create->options,
										"toast",
										validnsps,
										true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

	/* Create the "view" part of a materialized view. */
	if (is_matview)
	{
		/* StoreViewQuery scribbles on tree, so make a copy */
		Query	   *query = (Query *) copyObject(into->viewQuery);

		StoreViewQuery(intoRelationAddr.objectId, query, false);
		CommandCounterIncrement();
	}

	return intoRelationAddr;
}


/*
 * create_ctas_nodata
 *
 * Create CTAS or materialized view when WITH NO DATA is used, starting from
 * the targetlist of the SELECT or view definition.
 */
static ObjectAddress
create_ctas_nodata(List *tlist, IntoClause *into)
{
	List	   *attrList;
	ListCell   *t,
			   *lc;

	/*
	 * Build list of ColumnDefs from non-junk elements of the tlist.  If a
	 * column name list was specified in CREATE TABLE AS, override the column
	 * names in the query.  (Too few column names are OK, too many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	foreach(t, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(t);

		if (!tle->resjunk)
		{
			ColumnDef  *col;
			char	   *colname;

			if (lc)
			{
				colname = strVal(lfirst(lc));
				lc = lnext(into->colNames, lc);
			}
			else
				colname = tle->resname;

			col = makeColumnDef(colname,
								exprType((Node *) tle->expr),
								exprTypmod((Node *) tle->expr),
								exprCollation((Node *) tle->expr));

			/*
			 * It's possible that the column is of a collatable type but the
			 * collation could not be resolved, so double-check.  (We must
			 * check this here because DefineRelation would adopt the type's
			 * default collation rather than complaining.)
			 */
			if (!OidIsValid(col->collOid) &&
				type_is_collatable(col->typeName->typeOid))
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("no collation was derived for column \"%s\" with collatable type %s",
								col->colname,
								format_type_be(col->typeName->typeOid)),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));

			attrList = lappend(attrList, col);
		}
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/* Create the relation definition using the ColumnDef list */
	return create_ctas_internal(attrList, into);
}


/*
 * ExecCreateTableAs -- execute a CREATE TABLE AS command
 */
ObjectAddress
ExecCreateTableAs(ParseState *pstate, CreateTableAsStmt *stmt,
				  ParamListInfo params, QueryEnvironment *queryEnv,
				  QueryCompletion *qc)
{
	Query	   *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool		is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;

	if (stmt->if_not_exists)
	{
		Oid			nspid;

		nspid = RangeVarGetCreationNamespace(stmt->into->rel);

		if (get_relname_relid(stmt->into->rel->relname, nspid))
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							stmt->into->rel->relname)));
			return InvalidObjectAddress;
		}
	}

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	dest = CreateIntoRelDestReceiver(into);

	/*
	 * The contained Query could be a SELECT, or an EXECUTE utility command.
	 * If the latter, we just pass it off to ExecuteQuery.
	 */
	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, ExecuteStmt))
	{
		ExecuteStmt *estmt = castNode(ExecuteStmt, query->utilityStmt);

		Assert(!is_matview);	/* excluded by syntax */
		ExecuteQuery(pstate, estmt, into, params, dest, qc);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		return address;
	}
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	if (is_matview && into->ivm)
	{
		check_ivm_restriction_context ctx = {false, false, false, NIL};

		if(contain_mutable_functions((Node *)query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mutable function is not supported on incrementally maintainable materialized view"),
					 errhint("functions must be marked IMMUTABLE")));

		check_ivm_restriction_walker((Node *) query, &ctx, 0);
		query = rewriteQueryForIMMV(query, into->colNames);
	}

	if (into->skipData)
	{
		/*
		 * If WITH NO DATA was specified, do not go through the rewriter,
		 * planner and executor.  Just define the relation using a code path
		 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
		 * from running the planner before all dependencies are set up.
		 */
		address = create_ctas_nodata(query->targetList, into);
	}
	else
	{
		/*
		 * Parse analysis was done already, but we still have to run the rule
		 * rewriter.  We do not do AcquireRewriteLocks: we assume the query
		 * either came straight from the parser, or suitable locks were
		 * acquired by plancache.c.
		 *
		 * Because the rewriter and planner tend to scribble on the input, we
		 * make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that CTAS is in a portal or plpgsql function
		 * and is executed repeatedly.  (See also the same hack in EXPLAIN and
		 * PREPARE.)
		 */

		rewritten = QueryRewrite(copyObject(query));

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for %s",
				 is_matview ? "CREATE MATERIALIZED VIEW" :
				 "CREATE TABLE AS SELECT");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plan = pg_plan_query(query, pstate->p_sourcetext,
							 CURSOR_OPT_PARALLEL_OK, params);

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
									dest, params, queryEnv, 0);

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* save the rowcount if we're given a qc to fill */
		if (qc)
			SetQueryCompletion(qc, CMDTAG_SELECT, queryDesc->estate->es_processed);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		/* and clean up */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		FreeQueryDesc(queryDesc);

		PopActiveSnapshot();
	}

	if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);


		if (into->ivm)
		{
			Oid matviewOid = address.objectId;
			Relation matviewRel = table_open(matviewOid, NoLock);
			Relids	relids = NULL;

			/*
			 * Mark relisivm field, if it's a matview and into->ivm is true.
			 */
			SetMatViewIVMState(matviewRel, true);

			if (!into->skipData)
			{
				CreateIvmTriggersOnBaseTables(query, (Node *)query->jointree, matviewOid, &relids);
				bms_free(relids);
			}
			table_close(matviewRel, NoLock);
		}
	}

	return address;
}

/*
 * rewriteQueryForIMMV -- rewrite view definition query for IMMV
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
	 * If this query has EXISTS clause, rewrite query and
	 * add __ivm_exists_count_X__ column.
	 */
	if (rewritten->hasSubLinks)
	{
		ListCell *lc;
		RangeTblEntry *rte;
		int varno = 0;

		/* rewrite EXISTS sublink to LATERAL subquery */
		rewrite_query_for_exists_subquery(rewritten);

		/* Add count(*) using EXISTS clause */
		foreach(lc, rewritten->rtable)
		{
			char *columnName;
			int attnum;
			Node *countCol = NULL;
			varno++;

			rte = (RangeTblEntry *) lfirst(lc);
			if (!rte->subquery || !rte->lateral)
				continue;
			pstate->p_rtable = rewritten->rtable;

			columnName = getColumnNameStartWith(rte, "__ivm_exists", &attnum);
			if (columnName == NULL)
				continue;
			countCol = (Node *)makeVar(varno ,attnum,
						INT8OID, -1, InvalidOid, 0);


			if (countCol != NULL)
			{
				tle = makeTargetEntry((Expr *) countCol,
											list_length(rewritten->targetList) + 1,
											pstrdup(columnName),
											false);
				rewritten->targetList = list_concat(rewritten->targetList, list_make1(tle));
			}
		}
	}

	/* group keys must be in targetlist */
	if (rewritten->groupClause)
	{
		ListCell *lc;
		foreach(lc, rewritten->groupClause)
		{
			SortGroupClause *scl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(scl, rewritten->targetList);

			if (tle->resjunk)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("GROUP BY expression not appeared in select list is not supported on incrementally maintainable materialized view")));
		}
	}
	else if (!rewritten->hasAggs && rewritten->distinctClause)
		rewritten->groupClause = transformDistinctClause(NULL, &rewritten->targetList, rewritten->sortClause, false);


	if (rewritten->hasAggs)
	{
		ListCell *lc;
		List *agg_counts = NIL;
		AttrNumber next_resno = list_length(rewritten->targetList) + 1;
		Const	*dmy_arg = makeConst(INT4OID,
									 -1,
									 InvalidOid,
									 sizeof(int32),
									 Int32GetDatum(1),
									 false,
									 true); /* pass by value */

		foreach(lc, rewritten->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			TargetEntry *tle_count;
			char *resname = (colNames == NIL ? tle->resname : strVal(list_nth(colNames, tle->resno-1)));


			if (IsA(tle->expr, Aggref))
			{
				Aggref *aggref = (Aggref *) tle->expr;
				const char *aggname = get_func_name(aggref->aggfnoid);

				/*
				 * For aggregate functions except to count, add count func with the same arg parameters.
				 * Also, add sum func for agv.
				 *
				 * XXX: If there are same expressions explicitly in the target list, we can use this instead
				 * of adding new duplicated one.
				 */
				if (strcmp(aggname, "count") != 0)
				{
					fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);

					/* Make a Func with a dummy arg, and then override this by the original agg's args. */
					node = ParseFuncOrColumn(pstate, fn->funcname, list_make1(dmy_arg), NULL, fn, false, -1);
					((Aggref *)node)->args = aggref->args;

					tle_count = makeTargetEntry((Expr *) node,
												next_resno,
												pstrdup(makeObjectName("__ivm_count",resname, "_")),
												false);
					agg_counts = lappend(agg_counts, tle_count);
					next_resno++;
				}
				if (strcmp(aggname, "avg") == 0)
				{
					List *dmy_args = NIL;
					ListCell *lc;
					foreach(lc, aggref->aggargtypes)
					{
						Oid		typeid = lfirst_oid(lc);
						Type	type = typeidType(typeid);

						Const *con = makeConst(typeid,
											   -1,
											   typeTypeCollation(type),
											   typeLen(type),
											   (Datum) 0,
											   true,
											   typeByVal(type));
						dmy_args = lappend(dmy_args, con);
						ReleaseSysCache(type);

					}
					fn = makeFuncCall(list_make1(makeString("sum")), NIL, -1);

					/* Make a Func with a dummy arg, and then override this by the original agg's args. */
					node = ParseFuncOrColumn(pstate, fn->funcname, dmy_args, NULL, fn, false, -1);
					((Aggref *)node)->args = aggref->args;

					tle_count = makeTargetEntry((Expr *) node,
												next_resno,
												pstrdup(makeObjectName("__ivm_sum",resname, "_")),
												false);
					agg_counts = lappend(agg_counts, tle_count);
					next_resno++;
				}

			}
		}
		rewritten->targetList = list_concat(rewritten->targetList, agg_counts);

	}

	/* Add count(*) for counting algorithm */
	if (rewritten->distinctClause || rewritten->hasAggs)
	{
		fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
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

/*
 * CreateIvmTriggersOnBaseTables -- create IVM triggers on all base tables
 */
void
CreateIvmTriggersOnBaseTables(Query *qry, Node *jtnode, Oid matviewOid, Relids *relids)
{
	if (jtnode == NULL)
		return;
	if (IsA(jtnode, RangeTblRef))
	{
		int			rti = ((RangeTblRef *) jtnode)->rtindex;
		RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

		if (rte->rtekind == RTE_RELATION)
		{
			if (!bms_is_member(rte->relid, *relids))
			{
				CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE);
				CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_BEFORE);
				CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_BEFORE);
				CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER);
				CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER);
				CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER);

				*relids = bms_add_member(*relids, rte->relid);
			}
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			Query *subquery = rte->subquery;
			Assert(rte->subquery != NULL);

			CreateIvmTriggersOnBaseTables(subquery, (Node *)subquery->jointree, matviewOid, relids);
		}
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;

		foreach(l, f->fromlist)
			CreateIvmTriggersOnBaseTables(qry, lfirst(l), matviewOid, relids);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		CreateIvmTriggersOnBaseTables(qry, j->larg, matviewOid, relids);
		CreateIvmTriggersOnBaseTables(qry, j->rarg, matviewOid, relids);
	}
	else
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));
}

/*
 * GetIntoRelEFlags --- compute executor flags needed for CREATE TABLE AS
 *
 * This is exported because EXPLAIN and PREPARE need it too.  (Note: those
 * callers still need to deal explicitly with the skipData flag; since they
 * use different methods for suppressing execution, it doesn't seem worth
 * trying to encapsulate that part.)
 */
int
GetIntoRelEFlags(IntoClause *intoClause)
{
	int			flags = 0;

	if (intoClause->skipData)
		flags |= EXEC_FLAG_WITH_NO_DATA;

	return flags;
}

/*
 * CreateIntoRelDestReceiver -- create a suitable DestReceiver object
 *
 * intoClause will be NULL if called from CreateDestReceiver(), in which
 * case it has to be provided later.  However, it is convenient to allow
 * self->into to be filled in immediately for other callers.
 */
DestReceiver *
CreateIntoRelDestReceiver(IntoClause *intoClause)
{
	DR_intorel *self = (DR_intorel *) palloc0(sizeof(DR_intorel));

	self->pub.receiveSlot = intorel_receive;
	self->pub.rStartup = intorel_startup;
	self->pub.rShutdown = intorel_shutdown;
	self->pub.rDestroy = intorel_destroy;
	self->pub.mydest = DestIntoRel;
	self->into = intoClause;
	/* other private fields will be set during intorel_startup */

	return (DestReceiver *) self;
}

/*
 * intorel_startup --- executor startup
 */
static void
intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_intorel *myState = (DR_intorel *) self;
	IntoClause *into = myState->into;
	bool		is_matview;
	char		relkind;
	List	   *attrList;
	ObjectAddress intoRelationAddr;
	Relation	intoRelationDesc;
	RangeTblEntry *rte;
	ListCell   *lc;
	int			attnum;

	Assert(into != NULL);		/* else somebody forgot to set it */

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	/*
	 * Build column definitions using "pre-cooked" type and collation info. If
	 * a column name list was specified in CREATE TABLE AS, override the
	 * column names derived from the query.  (Too few column names are OK, too
	 * many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	for (attnum = 0; attnum < typeinfo->natts; attnum++)
	{
		Form_pg_attribute attribute = TupleDescAttr(typeinfo, attnum);
		ColumnDef  *col;
		char	   *colname;

		if (lc)
		{
			colname = strVal(lfirst(lc));
			lc = lnext(into->colNames, lc);
		}
		else
			colname = NameStr(attribute->attname);

		col = makeColumnDef(colname,
							attribute->atttypid,
							attribute->atttypmod,
							attribute->attcollation);

		/*
		 * It's possible that the column is of a collatable type but the
		 * collation could not be resolved, so double-check.  (We must check
		 * this here because DefineRelation would adopt the type's default
		 * collation rather than complaining.)
		 */
		if (!OidIsValid(col->collOid) &&
			type_is_collatable(col->typeName->typeOid))
			ereport(ERROR,
					(errcode(ERRCODE_INDETERMINATE_COLLATION),
					 errmsg("no collation was derived for column \"%s\" with collatable type %s",
							col->colname,
							format_type_be(col->typeName->typeOid)),
					 errhint("Use the COLLATE clause to set the collation explicitly.")));

		attrList = lappend(attrList, col);
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/*
	 * Actually create the target table
	 */
	intoRelationAddr = create_ctas_internal(attrList, into);

	/*
	 * Finally we can open the target table
	 */
	intoRelationDesc = table_open(intoRelationAddr.objectId, AccessExclusiveLock);

	/*
	 * Check INSERT permission on the constructed table.
	 *
	 * XXX: It would arguably make sense to skip this check if into->skipData
	 * is true.
	 */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = intoRelationAddr.objectId;
	rte->relkind = relkind;
	rte->rellockmode = RowExclusiveLock;
	rte->requiredPerms = ACL_INSERT;

	for (attnum = 1; attnum <= intoRelationDesc->rd_att->natts; attnum++)
		rte->insertedCols = bms_add_member(rte->insertedCols,
										   attnum - FirstLowInvalidHeapAttributeNumber);

	ExecCheckRTPerms(list_make1(rte), true);

	/*
	 * Make sure the constructed table does not have RLS enabled.
	 *
	 * check_enable_rls() will ereport(ERROR) itself if the user has requested
	 * something invalid, and otherwise will return RLS_ENABLED if RLS should
	 * be enabled here.  We don't actually support that currently, so throw
	 * our own ereport(ERROR) if that happens.
	 */
	if (check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("policies not yet implemented for this command")));

	/*
	 * Tentatively mark the target as populated, if it's a matview and we're
	 * going to fill it; otherwise, no change needed.
	 */
	if (is_matview && !into->skipData)
		SetMatViewPopulatedState(intoRelationDesc, true);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->rel = intoRelationDesc;
	myState->reladdr = intoRelationAddr;
	myState->output_cid = GetCurrentCommandId(true);
	myState->ti_options = TABLE_INSERT_SKIP_FSM;
	myState->bistate = GetBulkInsertState();

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber);
}

/*
 * intorel_receive --- receive one tuple
 */
static bool
intorel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;

	/*
	 * Note that the input slot might not be of the type of the target
	 * relation. That's supported by table_tuple_insert(), but slightly less
	 * efficient than inserting with the right slot - but the alternative
	 * would be to copy into a slot of the right type, which would not be
	 * cheap either. This also doesn't allow accessing per-AM data (say a
	 * tuple's xmin), but since we don't do that here...
	 */

	table_tuple_insert(myState->rel,
					   slot,
					   myState->output_cid,
					   myState->ti_options,
					   myState->bistate);

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * intorel_shutdown --- executor end
 */
static void
intorel_shutdown(DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;

	FreeBulkInsertState(myState->bistate);

	table_finish_bulk_insert(myState->rel, myState->ti_options);

	/* close rel, but keep lock until commit */
	table_close(myState->rel, NoLock);
	myState->rel = NULL;
}

/*
 * intorel_destroy --- release DestReceiver object
 */
static void
intorel_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateIvmTrigger -- create IVM trigger on a base table
 */
static void
CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing)
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
			n->name = "ivm_newtable";
			n->isNew = true;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
		if (type == TRIGGER_TYPE_DELETE || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "ivm_oldtable";
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
	ivm_trigger->args = list_make1(makeString(
		DatumGetPointer(DirectFunctionCall1(oidout, ObjectIdGetDatum(viewOid)))));

	address = CreateTrigger(ivm_trigger, NULL, relOid, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, InvalidOid, NULL, true, false);

	recordDependencyOn(&address, &refaddr, DEPENDENCY_IMMV);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}


/*
 * check_ivm_restriction_walker --- look for specify nodes in the query tree
 */
static void
check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *ctx, int depth)
{
	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *qry = (Query *)node;
				ListCell   *lc;
				/* if contained CTE, return error */
				if (qry->cteList != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CTE is not supported on incrementally maintainable materialized view")));
				if (qry->havingQual != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg(" HAVING clause is not supported on incrementally maintainable materialized view")));
				if (qry->sortClause != NIL)	/* There is a possibility that we don't need to return an error */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ORDER BY clause is not supported on incrementally maintainable materialized view")));
				if (qry->limitOffset != NULL || qry->limitCount != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("LIMIT/OFFSET clause is not supported on incrementally maintainable materialized view")));
				if (qry->hasDistinctOn)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DISTINCT ON is not supported on incrementally maintainable materialized view")));
				if (qry->hasWindowFuncs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("window functions are not supported on incrementally maintainable materialized view")));
				if (qry->groupingSets != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("GROUPING SETS, ROLLUP, or CUBE clauses is not supported on incrementally maintainable materialized view")));
				if (qry->setOperations != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNION/INTERSECT/EXCEPT statements are not supported on incrementally maintainable materialized view")));
				if (list_length(qry->targetList) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("empty target list is not supported on incrementally maintainable materialized view")));
				if (qry->rowMarks != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("FOR UPDATE/SHARE clause is not supported on incrementally maintainable materialized view")));

				/* subquery restrictions */
				if (depth > 0 && qry->distinctClause != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DISTINCT cluase in nested query are not supported on incrementally maintainable materialized view")));
				if (depth > 0 && qry->hasAggs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate functions in nested query are not supported on incrementally maintainable materialized view")));

				ctx->has_agg = qry->hasAggs;

				/* if contained VIEW or subquery into RTE, return error */
				foreach(lc, qry->rtable)
				{
					RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

					if (rte->tablesample != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("TABLESAMPLE clause is not supported on incrementally maintainable materialized view")));
					if (rte->relkind == RELKIND_RELATION && find_inheritance_children(rte->relid, NoLock) != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("inheritance parent is not supported on incrementally maintainable materialized view")));
					if (rte->relkind == RELKIND_VIEW ||
							rte->relkind == RELKIND_MATVIEW)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("VIEW or MATERIALIZED VIEW is not supported on incrementally maintainable materialized view")));

					if (rte->rtekind ==  RTE_SUBQUERY)
					{
						if (ctx->has_outerjoin)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("subquery is not supported with outer join")));

						ctx->has_subquery = true;
						check_ivm_restriction_walker((Node *) rte->subquery, ctx, depth + 1);
					}
				}

				/* search in jointree */
				check_ivm_restriction_walker((Node *) qry->jointree, ctx, depth);

				/* search in target lists */
				foreach(lc, qry->targetList)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(lc);
					if (isIvmColumn(tle->resname))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("column name %s is not supported on incrementally maintainable materialized view", tle->resname)));
					if (ctx->has_agg && !IsA(tle->expr, Aggref) && contain_aggs_of_level((Node *) tle->expr, 0))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("expression containing an aggregate in it is not supported on incrementally maintainable materialized view")));
					check_ivm_restriction_walker((Node *) tle->expr, ctx, depth);
				}

				/* additional restriction checks for outer join query */
				if (ctx->has_outerjoin && depth == 0)
				{
					List	*where_quals_vars = NIL;
					List	*nonnullable_vars = find_nonnullable_vars((Node *) qry->jointree->quals);
					List	*qual_vars = NIL;
					ListCell *lc;

					foreach (lc, ctx->join_quals)
					{
						OpExpr	*op = (OpExpr *) lfirst(lc);

						if (!is_equijoin_condition(op))
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("this query is not allowed on incrementally maintainable materialized view"),
										 errhint("Only simple equijoin is supported with outer join")));

						op = (OpExpr *) flatten_join_alias_vars(qry, (Node *) op);
						qual_vars = list_concat(qual_vars, pull_vars_of_level((Node *) op, 0));
					}

					foreach (lc, qual_vars)
					{
						Var	*var = lfirst(lc);
						ListCell *lc2;
						bool found = false;

						foreach(lc2, qry->targetList)
						{
							TargetEntry	*tle = lfirst(lc2);

							if (IsA(tle->expr, Var))
							{
								Var *var2 = (Var *) flatten_join_alias_vars(qry, (Node *) tle->expr);
								if (var->varno == var2->varno && var->varattno == var2->varattno)
								{
									found = true;
									break;
								}
							}
						}
						if (!found)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("targetlist must contain vars in the join condition with outer join")));
					}

					where_quals_vars = pull_vars_of_level(flatten_join_alias_vars(qry, (Node *) qry->jointree->quals), 0);

					if (list_length(list_difference(where_quals_vars, nonnullable_vars)) > 0)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("this query is not allowed on incrementally maintainable materialized view"),
								 errhint("WHERE cannot contain non null-rejecting predicates with outer join")));

					if (contain_nonstrict_functions((Node *) qry->targetList))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("targetlist cannot contain non strict functions with outer join")));
				}

				break;
			}
		case T_JoinExpr:
			{
				JoinExpr *joinexpr = (JoinExpr *)node;
				if (IS_OUTER_JOIN(joinexpr->jointype))
				{
					if (ctx->has_subquery)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("this query is not allowed on incrementally maintainable materialized view"),
								 errhint("subquery is not supported with outer join")));
					if (ctx->has_agg)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("this query is not allowed on incrementally maintainable materialized view"),
								 errhint("aggregate is not supported with outer join")));

					ctx->has_outerjoin = true;
					ctx->join_quals = lappend(ctx->join_quals, joinexpr->quals);
				}
				/* left side */
				check_ivm_restriction_walker((Node *) joinexpr->larg, ctx, depth);
				/* right side */
				check_ivm_restriction_walker((Node *) joinexpr->rarg, ctx, depth);
				check_ivm_restriction_walker((Node *) joinexpr->quals, ctx, depth);
			}
			break;
		case T_FromExpr:
			{
				ListCell *lc;
				FromExpr *fromexpr = (FromExpr *)node;
				foreach(lc, fromexpr->fromlist)
				{
					check_ivm_restriction_walker((Node *) lfirst(lc), ctx, depth);
				}

				check_ivm_restriction_walker((Node *) fromexpr->quals, ctx, depth);
			}
			break;
		case T_Var:
			{
				/* if system column, return error */
				Var	*variable = (Var *) node;
				if (variable->varattno < 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("system column is not supported on incrementally maintainable materialized view")));
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				ListCell   *lc;

				foreach(lc, boolexpr->args)
				{
					Node	   *arg = (Node *) lfirst(lc);
					check_ivm_restriction_walker(arg, ctx, depth);
				}
				break;
			}
		case T_NullIfExpr: /* same as OpExpr */
		case T_DistinctExpr: /* same as OpExpr */
		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				ListCell   *lc;
				foreach(lc, op->args)
				{
					Node	   *arg = (Node *) lfirst(lc);
					check_ivm_restriction_walker(arg, ctx, depth);
				}
				break;
			}
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				ListCell *lc;
				/* result for ELSE clause */
				check_ivm_restriction_walker((Node *) caseexpr->defresult, ctx, depth);
				/* expr for WHEN clauses */
				foreach(lc, caseexpr->args)
				{
					CaseWhen *when = (CaseWhen *) lfirst(lc);
					Node *w_expr = (Node *) when->expr;
					/* result for WHEN clause */
					check_ivm_restriction_walker((Node *) when->result, ctx, depth);
					/* expr clause*/
					check_ivm_restriction_walker((Node *) w_expr, ctx, depth);
				}
				break;
			}
		case T_SubLink:
			{
				/* Now, EXISTS clause is supported only */
				SubLink	*sublink = (SubLink *) node;
				if (sublink->subLinkType != EXISTS_SUBLINK)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("this query is not allowed on incrementally maintainable materialized view"),
							 errhint("subquery in WHERE clause only supports subquery with EXISTS clause")));
				if (depth > 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("nested subquery is not supported on incrementally maintainable materialized view")));
				if (ctx->has_outerjoin)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("this query is not allowed on incrementally maintainable materialized view"),
							 errhint("subquery with outer join is not supported")));
				check_ivm_restriction_walker(sublink->subselect, ctx, depth + 1);
				break;
			}
		case T_Aggref:
			{
				/* Check if this supports IVM */
				Aggref *aggref = (Aggref *) node;
				const char *aggname = format_procedure(aggref->aggfnoid);

				if (aggref->aggfilter != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with FILTER clause is not supported on incrementally maintainable materialized view")));

				if (aggref->aggdistinct != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with DISTINCT arguments is not supported on incrementally maintainable materialized view")));

				if (aggref->aggorder != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with ORDER clause is not supported on incrementally maintainable materialized view")));

				if (!check_aggregate_supports_ivm(aggref->aggfnoid))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function %s is not supported on incrementally maintainable materialized view", aggname)));
				break;
			}
		case T_SubPlan:
		case T_GroupingFunc:
		case T_WindowFunc:
		case T_FuncExpr:
		case T_SQLValueFunction:
		case T_Const:
		case T_Param:
		default:
			/* do nothing */
			break;
	}

	return;
}

/*
 * is_equijoin_condition - check if all operators must be btree equality or hash equality
 */
static bool
is_equijoin_condition(OpExpr *op)
{
	Oid			opno;
	Node	   *left_expr;
	Node	   *right_expr;
	Relids		left_varnos;
	Relids		right_varnos;
	Oid			opinputtype;

	/* Is it a binary opclause? */
	if (!IsA(op, OpExpr) || list_length(op->args) != 2)
		return false;

	opno = op->opno;
	left_expr = linitial(op->args);
	right_expr = lsecond(op->args);
	left_varnos = pull_varnos(left_expr);
	right_varnos = pull_varnos(right_expr);
	opinputtype = exprType(left_expr);

	if (bms_num_members(left_varnos) != 1 || bms_num_members(right_varnos) != 1 ||
		bms_equal(left_varnos, right_varnos) !=0)
		return false;

	if (op_mergejoinable(opno, opinputtype) && get_mergejoin_opfamilies(opno) != NIL)
		return true;

	if (op_hashjoinable(opno, opinputtype))
		return true;

	return false;
}

/*
 * check_aggregate_supports_ivm
 *
 * Check if the given aggregate function is supporting IVM
 */
static bool
check_aggregate_supports_ivm(Oid aggfnoid)
{
	switch (aggfnoid)
	{
		/* count */
		case F_AGG_COUNT_ANY:
		case F_AGG_COUNT_:

		/* sum */
		case F_AGG_SUM_INT8:
		case F_AGG_SUM_INT4:
		case F_AGG_SUM_INT2:
		case F_AGG_SUM_FLOAT4:
		case F_AGG_SUM_FLOAT8:
		case F_AGG_SUM_MONEY:
		case F_AGG_SUM_INTERVAL:
		case F_AGG_SUM_NUMERIC:

		/* avg */
		case F_AGG_AVG_INT8:
		case F_AGG_AVG_INT4:
		case F_AGG_AVG_INT2:
		case F_AGG_AVG_NUMERIC:
		case F_AGG_AVG_FLOAT4:
		case F_AGG_AVG_FLOAT8:
		case F_AGG_AVG_INTERVAL:

		/* min */
		case F_AGG_MIN_ANYARRAY:
		case F_AGG_MIN_INT8:
		case F_AGG_MIN_INT4:
		case F_AGG_MIN_INT2:
		case F_AGG_MIN_OID:
		case F_AGG_MIN_FLOAT4:
		case F_AGG_MIN_FLOAT8:
		case F_AGG_MIN_DATE:
		case F_AGG_MIN_TIME:
		case F_AGG_MIN_TIMETZ:
		case F_AGG_MIN_MONEY:
		case F_AGG_MIN_TIMESTAMP:
		case F_AGG_MIN_TIMESTAMPTZ:
		case F_AGG_MIN_INTERVAL:
		case F_AGG_MIN_TEXT:
		case F_AGG_MIN_NUMERIC:
		case F_AGG_MIN_BPCHAR:
		case F_AGG_MIN_TID:
		case F_AGG_MIN_ANYENUM:
		case F_AGG_MIN_INET:
		case F_AGG_MIN_PG_LSN:

		/* max */
		case F_AGG_MAX_ANYARRAY:
		case F_AGG_MAX_INT8:
		case F_AGG_MAX_INT4:
		case F_AGG_MAX_INT2:
		case F_AGG_MAX_OID:
		case F_AGG_MAX_FLOAT4:
		case F_AGG_MAX_FLOAT8:
		case F_AGG_MAX_DATE:
		case F_AGG_MAX_TIME:
		case F_AGG_MAX_TIMETZ:
		case F_AGG_MAX_MONEY:
		case F_AGG_MAX_TIMESTAMP:
		case F_AGG_MAX_TIMESTAMPTZ:
		case F_AGG_MAX_INTERVAL:
		case F_AGG_MAX_TEXT:
		case F_AGG_MAX_NUMERIC:
		case F_AGG_MAX_BPCHAR:
		case F_AGG_MAX_TID:
		case F_AGG_MAX_ANYENUM:
		case F_AGG_MAX_INET:
		case F_AGG_MAX_PG_LSN:
			return true;

		default:
			return false;
	}
}

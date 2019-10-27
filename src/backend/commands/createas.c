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
 * we must return a tuples-processed count in the completionTag.  (We no
 * longer do that for CTAS ... WITH NO DATA, however.)
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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
#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_clause.h"
#include "rewrite/rewriteHandler.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"

#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parse_relation.h"
#include "nodes/print.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "commands/defrem.h"
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

/* utility functions for CTAS definition creation */
static ObjectAddress create_ctas_internal(List *attrList, IntoClause *into);
static ObjectAddress create_ctas_nodata(List *tlist, IntoClause *into);

/* DestReceiver routines for collecting data */
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown(DestReceiver *self);
static void intorel_destroy(DestReceiver *self);

static void CreateIvmTrigger(Oid relOid, Oid viewOid, char *matviewname, int16 type, int16 timing, char *count_col);
static void CreateIvmTriggersOnBaseTables(Query *qry, Node *jtnode, Oid matviewOid, char* matviewname, Bitmapset **relid_map, bool in_subquery);
static void check_ivm_restriction_walker(Node *node);

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
 * convert_EXISTS_subkink_to_lateral_join
 */
RangeTblEntry *
convert_EXISTS_sublink_to_lateral_join(Query *query)
{
	FromExpr *fromexpr;
	SubLink *sublink;

	RangeTblEntry *rte;
	RangeTblRef *rtr;
	Alias *alias;
	ParseState *pstate;
	Query *subselect;


	TargetEntry *tle_count;
	FuncCall *fn;
	Node *node;
	Expr *opexpr;

	/*
	 * This is test pattern. its pattern contain only subquery on WHERE clause.
	 * In the feature, other expr with subquery will be supported.
	 */
	fromexpr = (FromExpr *)query->jointree;
	sublink = (SubLink *)fromexpr->quals;

	subselect = (Query *)sublink->subselect;
	/* Currently, EXISTS is only supported */
	if (sublink->subLinkType != EXISTS_SUBLINK)
		return NULL;
	if (subselect->cteList)
		return NULL;
	/* */
	pstate = make_parsestate(NULL);
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * convert EXISTS subquery into LATERAL subquery in FROM clause.
	 */

	/* add COUNT(*) for counting exists condition */
	fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
	fn->agg_star = true;
	node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);
	tle_count = makeTargetEntry((Expr *) node,
								list_length(subselect->targetList) + 1,
								pstrdup("__ivm_exists_count__"),
								false);
	/* add __ivm_exists_count__ column */
	subselect->targetList = list_concat(subselect->targetList, list_make1(tle_count));
	subselect->hasAggs = true;

	/* add subquery in from clause */
	alias = makeAlias("subquery_with_exists", NIL);
	/* it means that LATERAL is enable if fourth argument is true */
	rte = addRangeTableEntryForSubquery(pstate,subselect,alias,true,true);
	query->rtable = lappend(query->rtable, rte);
	pstate->p_rtable = query->rtable; 

	rtr = makeNode(RangeTblRef);
	/* assume new rte is at end */
	rtr->rtindex = list_length(query->rtable);

	((FromExpr *)query->jointree)->fromlist = lappend(((FromExpr *)query->jointree)->fromlist, rtr);


	fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
	fn->agg_star = true;

	node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);


	/*
	 * it means using int84gt( '>' operator). it will be replaced to make_op().
	 */
	opexpr = make_opclause(419, BOOLOID, false,
					(Expr *)node,
					(Expr *)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(0), false, true),
					InvalidOid, InvalidOid);
	fix_opfuncids((Node *) opexpr);
	/* drop subquery in WHERE clause */
	fromexpr->quals = NULL;
	query->hasSubLinks = false;

	subselect->havingQual = (Node *)opexpr;

	return rte;
}


/*
 * ExecCreateTableAs -- execute a CREATE TABLE AS command
 */
ObjectAddress
ExecCreateTableAs(CreateTableAsStmt *stmt, const char *queryString,
				  ParamListInfo params, QueryEnvironment *queryEnv,
				  char *completionTag)
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
	Query	   *copied_query;
	bool hasSublink = false;

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

	if (is_matview && into->ivm)
		check_ivm_restriction_walker((Node *) query);

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
		ExecuteQuery(estmt, into, queryString, params, dest, completionTag);

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

		copied_query = copyObject(query);
		if (is_matview && into->ivm)
		{
			TargetEntry *tle;
			Node *node;
			ParseState *pstate = make_parsestate(NULL);
			FuncCall *fn;

			pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

			/* If conatin EXISTS clause*/
			if (copied_query->hasSubLinks)
			{
				hasSublink = true;
				/*sub query to rtable  */
				convert_EXISTS_sublink_to_lateral_join(copied_query);
			}

			/* Add count(*) using EXISTS clause */
			if (hasSublink)
			{
				ListCell *lc;
				RangeTblEntry *rte;
				Node *colCountSubquery;
				/* search subquery in RangeTblEntry */
				foreach(lc, copied_query->rtable)
				{
					rte = (RangeTblEntry *) lfirst(lc);
					if (rte->subquery)
					{
						pstate->p_rtable = copied_query->rtable;
						colCountSubquery = scanRTEForColumn(pstate,rte,"__ivm_exists_count__",-1, 0, NULL);
						break;
					}
				}
				Assert(colCountSubquery != NULL);
				tle = makeTargetEntry((Expr *) colCountSubquery,
											list_length(copied_query->targetList) + 1,
											pstrdup("__ivm_exists_count__"),
											false);
				copied_query->targetList = list_concat(copied_query->targetList, list_make1(tle));
			}

			/* group keys must be in targetlist */
			if (copied_query->groupClause)
			{
				ListCell *lc;
				foreach(lc, copied_query->groupClause)
				{
					SortGroupClause *scl = (SortGroupClause *) lfirst(lc);
					TargetEntry *tle = get_sortgroupclause_tle(scl, copied_query->targetList);

					if (tle->resjunk)
						elog(ERROR, "GROUP BY expression must appear in select list for incremental materialized views");
				}
			}
			else if (!copied_query->hasAggs)
				copied_query->groupClause = transformDistinctClause(NULL, &copied_query->targetList, copied_query->sortClause, false);
			if (copied_query->hasAggs)
			{
				ListCell *lc;
				List *agg_counts = NIL;
				AttrNumber next_resno = list_length(copied_query->targetList) + 1;
				Const	*dmy_arg = makeConst(INT4OID,
											 -1,
											 InvalidOid,
											 sizeof(int32),
											 Int32GetDatum(1),
											 false,
											 true); /* pass by value */

				foreach(lc, copied_query->targetList)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(lc);
					TargetEntry *tle_count;
					char *resname = (into->colNames == NIL ? tle->resname : strVal(list_nth(into->colNames, tle->resno-1)));


					if (IsA(tle->expr, Aggref))
					{
						Aggref *aggref = (Aggref *) tle->expr;
						const char *aggname = get_func_name(aggref->aggfnoid);

						/* XXX: need some generalization
						 *
						 * Specifically, Using func names is not robust.  We can use oids instead
						 * of names, but it would be nice to add some information to pg_aggregate.
						 */
						if (strcmp(aggname, "sum") !=0
							&& strcmp(aggname, "count") != 0
							&& strcmp(aggname, "avg") != 0
							&& strcmp(aggname, "min") != 0
							&& strcmp(aggname, "max") != 0
						)
							elog(ERROR, "aggregate function %s is not supported", aggname);

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
								Type 	type = typeidType(typeid);

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
				copied_query->targetList = list_concat(copied_query->targetList, agg_counts);

			}

			/* Add count(*) for counting algorithm */
			fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
			fn->agg_star = true;

			node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

			tle = makeTargetEntry((Expr *) node,
								  	list_length(copied_query->targetList) + 1,
								  	pstrdup("__ivm_count__"),
								  	false);
			copied_query->targetList = lappend(copied_query->targetList, tle);

			copied_query->hasAggs = true;
		}


		rewritten = QueryRewrite(copied_query);

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for %s",
				 is_matview ? "CREATE MATERIALIZED VIEW" :
				 "CREATE TABLE AS SELECT");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plan = pg_plan_query(query, CURSOR_OPT_PARALLEL_OK, params);

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
		queryDesc = CreateQueryDesc(plan, queryString,
									GetActiveSnapshot(), InvalidSnapshot,
									dest, params, queryEnv, 0);

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* save the rowcount if we're given a completionTag to fill */
		if (completionTag)
			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "SELECT " UINT64_FORMAT,
					 queryDesc->estate->es_processed);

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
			char	*matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
													 RelationGetRelationName(matviewRel));
			Bitmapset  *relid_map = NULL;

			copied_query = copyObject(query);
			AcquireRewriteLocks(copied_query, true, false);

			CreateIvmTriggersOnBaseTables(copied_query, (Node *)copied_query->jointree, matviewOid, matviewname, &relid_map, false);

			table_close(matviewRel, NoLock);

			bms_free(relid_map);
		}
	}

	return address;
}

static void CreateIvmTriggersOnBaseTables(Query *qry, Node *jtnode, Oid matviewOid, char* matviewname, Bitmapset **relid_map, bool in_subquery)
{

	if (jtnode == NULL)
		return;
	if (IsA(jtnode, RangeTblRef))
	{
		int			rti = ((RangeTblRef *) jtnode)->rtindex;
		RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

		if (rte->rtekind == RTE_RELATION)
		{
			if (!bms_is_member(rte->relid, *relid_map))
			{
				char *count_colname = in_subquery ? "__ivm_exists_count__" : "__ivm_count__";

				CreateIvmTrigger(rte->relid, matviewOid, matviewname, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE, count_colname);
				CreateIvmTrigger(rte->relid, matviewOid, matviewname, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_BEFORE, count_colname);
				CreateIvmTrigger(rte->relid, matviewOid, matviewname, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_BEFORE, count_colname);
				CreateIvmTrigger(rte->relid, matviewOid, matviewname, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, count_colname);
				CreateIvmTrigger(rte->relid, matviewOid, matviewname, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER, count_colname);
				CreateIvmTrigger(rte->relid, matviewOid, matviewname, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER, count_colname);

				*relid_map = bms_add_member(*relid_map, rte->relid);
			}
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			Query *subquery = rte->subquery;
			Assert(rte->subquery != NULL);
			/*
			 * On the condition, not allow a subquery in the subquery.
			 */
			if (in_subquery)
				elog(ERROR, "subquery is not supported in subquery with IVM");

			CreateIvmTriggersOnBaseTables(subquery, (Node *)subquery->jointree, matviewOid, matviewname, relid_map, true);
		}
		else
			elog(ERROR, "unsupported RTE kind: %d", (int) rte->rtekind);
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;

		foreach(l, f->fromlist)
			CreateIvmTriggersOnBaseTables(qry, lfirst(l), matviewOid, matviewname, relid_map, in_subquery);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		CreateIvmTriggersOnBaseTables(qry, j->larg, matviewOid, matviewname, relid_map, in_subquery);
		CreateIvmTriggersOnBaseTables(qry, j->rarg, matviewOid, matviewname, relid_map, in_subquery);
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
				 (errmsg("policies not yet implemented for this command"))));

	/*
	 * Tentatively mark the target as populated, if it's a matview and we're
	 * going to fill it; otherwise, no change needed.
	 */
	if (is_matview && !into->skipData)
		SetMatViewPopulatedState(intoRelationDesc, true);

	/*
	 * Mark relisivm field, if it's a matview and into->ivm is true.
	 */
	if (is_matview && into->ivm)
		SetMatViewIVMState(intoRelationDesc, true);
	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->rel = intoRelationDesc;
	myState->reladdr = intoRelationAddr;
	myState->output_cid = GetCurrentCommandId(true);

	/*
	 * We can skip WAL-logging the insertions, unless PITR or streaming
	 * replication is in use. We can skip the FSM in any case.
	 */
	myState->ti_options = TABLE_INSERT_SKIP_FSM |
		(XLogIsNeeded() ? 0 : TABLE_INSERT_SKIP_WAL);
	myState->bistate = GetBulkInsertState();

	/* Not using WAL requires smgr_targblock be initially invalid */
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


static void
CreateIvmTrigger(Oid relOid, Oid viewOid, char *matviewname, int16 type, int16 timing,char *count_col)
{
	CreateTrigStmt *ivm_trigger;
	List *transitionRels = NIL;
	ObjectAddress address, refaddr;

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
	ivm_trigger->args = list_make1(makeString(matviewname));
	ivm_trigger->args = lappend(ivm_trigger->args, makeString(count_col));

	address = CreateTrigger(ivm_trigger, NULL, relOid, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, InvalidOid, NULL, true, false);

	recordDependencyOn(&address, &refaddr, DEPENDENCY_AUTO);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

/*
 * check_ivm_restriction_walker --- look for specify nodes in the query tree
 */
static void
check_ivm_restriction_walker(Node *node)
{
	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	if (node == NULL)
		return;
	/*
	 * We currently don't support Sub-Query.
	 */
	if (IsA(node, SubPlan))
//	if (IsA(node, SubPlan) || IsA(node, SubLink))
		ereport(ERROR, (errmsg("subquery is not supported with IVM")));

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *qry = (Query *)node;
				ListCell   *lc;
				/* if contained CTE, return error */
				if (qry->cteList != NIL)
					ereport(ERROR, (errmsg("CTE is not supported with IVM")));

				/* if contained VIEW or subquery into RTE, return error */
				foreach(lc, qry->rtable)
				{
					RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
					if (rte->relkind == RELKIND_VIEW ||
							rte->relkind == RELKIND_MATVIEW)
						ereport(ERROR, (errmsg("VIEW or MATERIALIZED VIEW is not supported with IVM")));
//					if (rte->rtekind ==  RTE_SUBQUERY)
//						ereport(ERROR, (errmsg("subquery is not supported with IVM")));
				}

				/* search in jointree */
				check_ivm_restriction_walker((Node *) qry->jointree);

				/* search in target lists */
				foreach(lc, qry->targetList)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(lc);
					check_ivm_restriction_walker((Node *) tle->expr);
				}

				break;
			}
		case T_JoinExpr:
			{
				JoinExpr *joinexpr = (JoinExpr *)node;
				if (joinexpr->jointype > JOIN_INNER)
					ereport(ERROR, (errmsg("OUTER JOIN is not supported with IVM")));
				/* left side */
				check_ivm_restriction_walker((Node *) joinexpr->larg);
				/* right side */
				check_ivm_restriction_walker((Node *) joinexpr->rarg);
				check_ivm_restriction_walker((Node *) joinexpr->quals);
			}
			break;
		case T_FromExpr:
			{
				ListCell *lc;
				FromExpr *fromexpr = (FromExpr *)node;
				foreach(lc, fromexpr->fromlist)
				{
					check_ivm_restriction_walker((Node *) lfirst(lc));
				}
				check_ivm_restriction_walker((Node *) fromexpr->quals);
			}
			break;
		case T_Var:
			{
				/* if system column, return error */
				Var	*variable = (Var *) node;
				if (variable->varattno < 0)
					ereport(ERROR, (errmsg("system column is not supported with IVM")));
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				ListCell   *lc;

				foreach(lc, boolexpr->args)
				{
					Node	   *arg = (Node *) lfirst(lc);
					check_ivm_restriction_walker(arg);
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
					check_ivm_restriction_walker(arg);
				}
				break;
			}
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				ListCell *lc;
				/* result for ELSE clause */
				check_ivm_restriction_walker((Node *) caseexpr->defresult);
				/* expr for WHEN clauses */
				foreach(lc, caseexpr->args)
				{
					CaseWhen *when = (CaseWhen *) lfirst(lc);
					Node *w_expr = (Node *) when->expr;
					/* result for WHEN clause */
					check_ivm_restriction_walker((Node *) when->result);
					/* expr clause*/
					check_ivm_restriction_walker((Node *) w_expr);
				}
				break;
			}
		case T_SubLink:
			{
				/* Now, EXISTS clause is supported only */
 				SubLink	*sublink = (SubLink *) node;
				if (sublink->subLinkType != EXISTS_SUBLINK)
					ereport(ERROR, (errmsg("subquery is not supported with IVM, except for EXISTS clause")));
				break;
			}
		case T_SubPlan:
			{
				/* Now, not supported */
				break;
			}
		case T_Aggref:
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

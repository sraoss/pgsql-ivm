/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  materialized view support
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/createas.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rowsecurity.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Oid			transientoid;	/* OID of new heap into which to store */
	/* These fields are filled by transientrel_startup: */
	Relation	transientrel;	/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_transientrel;

#define MV_INIT_QUERYHASHSIZE	16

/* MV query type codes */
#define MV_PLAN_RECALC			1
#define MV_PLAN_SET_VALUE		2

/*
 * MI_QueryKey
 *
 * The key identifying a prepared SPI plan in our query hashtable
 */
typedef struct MV_QueryKey
{
	Oid			matview_id;	/* OID of materialized view */
	int32		query_type;	/* query type ID, see MV_PLAN_XXX above */
} MV_QueryKey;

/*
 * MV_QueryHashEntry
 *
 * Hash entry for cached plans used to maintain materialized views.
 */
typedef struct MV_QueryHashEntry
{
	MV_QueryKey key;
	SPIPlanPtr	plan;
} MV_QueryHashEntry;

/*
 * MV_TriggerHashEntry
 *
 * Hash entry for base tables on which IVM trigger is invoked
 */
typedef struct MV_TriggerHashEntry
{
	Oid	matview_id;			/* OID of the materialized view */
	int	before_trig_count;	/* count of before triggers invoked */
	int	after_trig_count;	/* count of after triggers invoked */

	Snapshot	snapshot;	/* Snapshot just before table change */

	List   *tables;		/* List of MV_TriggerTable */
	bool	has_old;	/* tuples are deleted from any table? */
	bool	has_new;	/* tuples are inserted into any table? */
} MV_TriggerHashEntry;

/*
 * MV_TriggerTable
 *
 * IVM related data for tables on which the trigger is invoked.
 */
typedef struct MV_TriggerTable
{
	Oid		table_id;			/* OID of the modified table */
	List   *old_tuplestores;	/* tuplestores for deleted tuples */
	List   *new_tuplestores;	/* tuplestores for inserted tuples */
	List   *old_rtes;			/* RTEs of ENRs for old_tuplestores*/
	List   *new_rtes;			/* RTEs of ENRs for new_tuplestores */

	List   *rte_paths;			/* List of paths to RTE of the modified table */
	RangeTblEntry *original_rte;	/* the original RTE saved before rewriting query */

	Relation	rel;			/* relation of the modified table */
	TupleTableSlot *slot;		/* for checking visibility in the pre-state table */
} MV_TriggerTable;

static HTAB *mv_query_cache = NULL;
static HTAB *mv_trigger_info = NULL;

static bool in_delta_calculation = false;

/* kind of IVM operation for the view */
typedef enum
{
	IVM_ADD,
	IVM_SUB
} IvmOp;

/* ENR name for materialized view delta */
#define NEW_DELTA_ENRNAME "new_delta"
#define OLD_DELTA_ENRNAME "old_delta"

/*
 * TermEffect
 *
 * Types of effect of how it affects each term in the maintenance graph when
 * a base table under outer join is modified.
 */
typedef enum
{
	IVM_DIRECT_EFFECT,
	IVM_INDIRECT_EFFECT,
	IVM_NO_EFFECT
} TermEffect;

/*
 * Term
 *
 * Term in normalized form of join expression. A normalized form is bag union
 * of terms and each term is a inner join of some base tables, which is null
 * extended due to antijoin with other base tables. Each term also has a
 * predicate for inner join.
 */
typedef struct
{
	Relids	relids;		/* relids of nonnullable tables */
	List	*quals;		/* predicate for inner join */
	List	*parents;	/* parent terms in maintenance graph */
	TermEffect	effect;	/* how affected by table change */
} Term;

/*
 * IvmMaintenanceGraph
 *
 * Node of view maintenance graph.
 */
typedef struct
{
	Term	*root;				/* root of graph which doesn't have parent */
	List	*terms;				/* list of terms in this graph */
	List	*vars_in_quals;		/* list of vars included in all terms' quals */
	List	*resnames_in_quals;	/* list of resnames corresponding vars_in_quals */
} IvmMaintenanceGraph;


static int	matview_maintenance_depth = 0;

static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);
static uint64 refresh_matview_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString);
static char *make_temptable_name_n(char *tempname, int n);
static void refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
								   int save_sec_context);
static void refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence);
static bool is_usable_unique_index(Relation indexRel);
static void OpenMatViewIncrementalMaintenance(void);
static void CloseMatViewIncrementalMaintenance(void);
static Query *get_matview_query(Relation matviewRel);

static Query *rewrite_query_for_preupdate_state(Query *query, List *tables,
								  ParseState *pstate, List *rte_path, Oid matviewid);
static void register_delta_ENRs(ParseState *pstate, Query *query, List *tables);
static char *make_delta_enr_name(const char *prefix, Oid relid, int count);
static RangeTblEntry *get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 QueryEnvironment *queryEnv, Oid matviewid);
static RangeTblEntry *union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix,
		   QueryEnvironment *queryEnv);
static Query *rewrite_query_for_distinct_and_aggregates(Query *query, ParseState *pstate);

static IvmMaintenanceGraph *make_maintenance_graph(Query *query, Relation matviewRel);
static List *get_normalized_form(Query *query, Node *jtnode);
static List *multiply_terms(Query *query, List *terms1, List *terms2, Node* qual);
static void update_maintenance_graph(IvmMaintenanceGraph *graph, int index);
static Query *rewrite_query_for_outerjoin(Query *query, int index, IvmMaintenanceGraph *graph);
static bool rewrite_jointype(Query *query, Node *node, int index);

static void calc_delta(MV_TriggerTable *table, List *rte_path, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new,
			TupleDesc *tupdesc_old, TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv);
static Query *rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, List *rte_path);
static ListCell *getRteListCell(Query *query, List *rte_path);

static void apply_delta(Oid matviewOid, Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname, IvmMaintenanceGraph *graph);
static void append_set_clause_for_count(const char *resname, StringInfo buf_old,
							StringInfo buf_new,StringInfo aggs_list);
static void append_set_clause_for_sum(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list);
static void append_set_clause_for_avg(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list,
						  const char *aggtype);
static void append_set_clause_for_minmax(const char *resname, StringInfo buf_old,
							 StringInfo buf_new, StringInfo aggs_list,
							 bool is_min);
static char *get_operation_string(IvmOp op, const char *col, const char *arg1, const char *arg2,
					 const char* count_col, const char *castType);
static char *get_null_condition_string(IvmOp op, const char *arg1, const char *arg2,
						  const char* count_col);
static void apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys);
static void apply_old_delta_with_count(const char *matviewname, const char *deltaname_old,
				List *keys, StringInfo aggs_list, StringInfo aggs_set,
				List *minmax_list, List *is_min_list,
				const char *count_colname,
				SPITupleTable **tuptable_recalc, uint64 *num_recalc);
static void apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list);
static void apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo target_list, StringInfo aggs_set,
				const char* count_colname);
static char *get_matching_condition_string(List *keys);
static char *get_returning_string(List *minmax_list, List *is_min_list, List *keys);
static char *get_minmax_recalc_condition_string(List *minmax_list, List *is_min_list);
static char *get_select_for_recalc_string(List *keys);
static void recalc_and_set_values(SPITupleTable *tuptable_recalc, int64 num_tuples,
					  List *namelist, List *keys, Relation matviewRel);
static SPIPlanPtr get_plan_for_recalc(Oid matviewOid, List *namelist, List *keys, Oid *keyTypes);
static SPIPlanPtr get_plan_for_set_values(Oid matviewOid, char *matviewname, List *namelist,
						Oid *valTypes);
static void insert_dangling_tuples(IvmMaintenanceGraph *graph, Query *query,
					   Relation matviewRel, const char *deltaname_old,
					   bool use_count);
static void delete_dangling_tuples(IvmMaintenanceGraph *graph, Query *query,
					   Relation matviewRel, const char *deltaname_new);
static void generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop);

static void mv_InitHashTables(void);
static SPIPlanPtr mv_FetchPreparedPlan(MV_QueryKey *key);
static void mv_HashPreparedPlan(MV_QueryKey *key, SPIPlanPtr plan);
static void mv_BuildQueryKey(MV_QueryKey *key, Oid matview_id, int32 query_type);
static void clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry, bool is_abort);

static List *get_securityQuals(Oid relId, int rt_index, Query *query);

/*
 * SetMatViewPopulatedState
 *		Mark a materialized view as populated, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewPopulatedState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relispopulated = newstate;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

/*
 * SetMatViewIVMState
 *		Mark a materialized view as IVM, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewIVMState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relisivm = newstate;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenumbers of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field shows whether the clause was used.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The matview's "populated" state is changed based on whether the contents
 * reflect the result set of the materialized view's query.
 */
ObjectAddress
ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
				   ParamListInfo params, QueryCompletion *qc)
{
	Oid			matviewOid;
	Relation	matviewRel;
	Query	   *dataQuery;
	Query	   *viewQuery;
	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDNewHeap;
	DestReceiver *dest;
	uint64		processed = 0;
	bool		concurrent;
	LOCKMODE	lockmode;
	char		relpersistence;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	ObjectAddress address;
	bool oldPopulated;

	/* Determine strength of lock needed. */
	concurrent = stmt->concurrent;
	lockmode = concurrent ? ExclusiveLock : AccessExclusiveLock;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(stmt->relation,
										  lockmode, 0,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = table_open(matviewOid, NoLock);
	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also lock down security-restricted operations and arrange to
	 * make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	oldPopulated = RelationIsPopulated(matviewRel);

	/* Make sure it is a materialized view. */
	if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a materialized view",
						RelationGetRelationName(matviewRel))));

	/* Check that CONCURRENTLY is not specified if not populated. */
	if (concurrent && !RelationIsPopulated(matviewRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CONCURRENTLY cannot be used when the materialized view is not populated")));

	/* Check that conflicting options have not been specified. */
	if (concurrent && stmt->skipData)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s and %s options cannot be used together",
						"CONCURRENTLY", "WITH NO DATA")));


	viewQuery = get_matview_query(matviewRel);

	/* For IMMV, we need to rewrite matview query */
	if (!stmt->skipData && RelationIsIVM(matviewRel))
		dataQuery = rewriteQueryForIMMV(viewQuery,NIL);
	else
		dataQuery = viewQuery;

	/*
	 * Check that there is a unique index with no WHERE clause on one or more
	 * columns of the materialized view if CONCURRENTLY is specified.
	 */
	if (concurrent)
	{
		List	   *indexoidlist = RelationGetIndexList(matviewRel);
		ListCell   *indexoidscan;
		bool		hasUniqueIndex = false;

		foreach(indexoidscan, indexoidlist)
		{
			Oid			indexoid = lfirst_oid(indexoidscan);
			Relation	indexRel;

			indexRel = index_open(indexoid, AccessShareLock);
			hasUniqueIndex = is_usable_unique_index(indexRel);
			index_close(indexRel, AccessShareLock);
			if (hasUniqueIndex)
				break;
		}

		list_free(indexoidlist);

		if (!hasUniqueIndex)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot refresh materialized view \"%s\" concurrently",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
													   RelationGetRelationName(matviewRel))),
					 errhint("Create a unique index with no WHERE clause on one or more columns of the materialized view.")));
	}

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

	/*
	 * Tentatively mark the matview as populated or not (this will roll back
	 * if we fail later).
	 */
	SetMatViewPopulatedState(matviewRel, !stmt->skipData);

	/* Concurrent refresh builds new data in temp tablespace, and does diff. */
	if (concurrent)
	{
		tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
		relpersistence = RELPERSISTENCE_TEMP;
	}
	else
	{
		tableSpace = matviewRel->rd_rel->reltablespace;
		relpersistence = matviewRel->rd_rel->relpersistence;
	}

	/* delete IMMV triggers. */
	if (RelationIsIVM(matviewRel) && stmt->skipData )
	{
		Relation	tgRel;
		Relation	depRel;
		ScanKeyData key;
		SysScanDesc scan;
		HeapTuple	tup;
		ObjectAddresses *immv_triggers;

		immv_triggers = new_object_addresses();

		tgRel = table_open(TriggerRelationId, RowExclusiveLock);
		depRel = table_open(DependRelationId, RowExclusiveLock);

		/* search triggers that depends on IMMV. */
		ScanKeyInit(&key,
					Anum_pg_depend_refobjid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(matviewOid));
		scan = systable_beginscan(depRel, DependReferenceIndexId, true,
								  NULL, 1, &key);
		while ((tup = systable_getnext(scan)) != NULL)
		{
			ObjectAddress obj;
			Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

			if (foundDep->classid == TriggerRelationId)
			{
				HeapTuple	tgtup;
				ScanKeyData tgkey[1];
				SysScanDesc tgscan;
				Form_pg_trigger tgform;

				/* Find the trigger name. */
				ScanKeyInit(&tgkey[0],
							Anum_pg_trigger_oid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(foundDep->objid));

				tgscan = systable_beginscan(tgRel, TriggerOidIndexId, true,
											NULL, 1, tgkey);
				tgtup = systable_getnext(tgscan);
				if (!HeapTupleIsValid(tgtup))
					elog(ERROR, "could not find tuple for immv trigger %u", foundDep->objid);

				tgform = (Form_pg_trigger) GETSTRUCT(tgtup);

				/* If trigger is created by IMMV, delete it. */
				if (strncmp(NameStr(tgform->tgname), "IVM_trigger_", 12) == 0)
				{
					obj.classId = foundDep->classid;
					obj.objectId = foundDep->objid;
					obj.objectSubId = foundDep->refobjsubid;
					add_exact_object_address(&obj, immv_triggers);
				}
				systable_endscan(tgscan);
			}
		}
		systable_endscan(scan);

		performMultipleDeletions(immv_triggers, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

		table_close(depRel, RowExclusiveLock);
		table_close(tgRel, RowExclusiveLock);
		free_object_addresses(immv_triggers);
	}

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	OIDNewHeap = make_new_heap(matviewOid, tableSpace,
							   matviewRel->rd_rel->relam,
							   relpersistence, ExclusiveLock);
	LockRelationOid(OIDNewHeap, AccessExclusiveLock);
	dest = CreateTransientRelDestReceiver(OIDNewHeap);

	/* Generate the data, if wanted. */
	if (!stmt->skipData)
		processed = refresh_matview_datafill(dest, dataQuery, NULL, NULL, queryString);

	/* Make the matview match the newly generated data. */
	if (concurrent)
	{
		int			old_depth = matview_maintenance_depth;

		PG_TRY();
		{
			refresh_by_match_merge(matviewOid, OIDNewHeap, relowner,
								   save_sec_context);
		}
		PG_CATCH();
		{
			matview_maintenance_depth = old_depth;
			PG_RE_THROW();
		}
		PG_END_TRY();
		Assert(matview_maintenance_depth == old_depth);
	}
	else
	{
		refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

		/*
		 * Inform cumulative stats system about our activity: basically, we
		 * truncated the matview and inserted some new data.  (The concurrent
		 * code path above doesn't need to worry about this because the
		 * inserts and deletes it issues get counted by lower-level code.)
		 */
		pgstat_count_truncate(matviewRel);
		if (!stmt->skipData)
			pgstat_count_heap_insert(matviewRel, processed);
	}

	if (!stmt->skipData && RelationIsIVM(matviewRel) && !oldPopulated)
	{
		CreateIndexOnIMMV(viewQuery, matviewRel, false);
		CreateIvmTriggersOnBaseTables(dataQuery, matviewOid, false);
	}

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	ObjectAddressSet(address, RelationRelationId, matviewOid);

	/*
	 * Save the rowcount so that pg_stat_statements can track the total number
	 * of rows processed by REFRESH MATERIALIZED VIEW command. Note that we
	 * still don't display the rowcount in the command completion tag output,
	 * i.e., the display_rowcount flag of CMDTAG_REFRESH_MATERIALIZED_VIEW
	 * command tag is left false in cmdtaglist.h. Otherwise, the change of
	 * completion tag output might break applications using it.
	 */
	if (qc)
		SetQueryCompletion(qc, CMDTAG_REFRESH_MATERIALIZED_VIEW, processed);

	return address;
}

/*
 * refresh_matview_datafill
 *
 * Execute the given query, sending result rows to "dest" (which will
 * insert them into the target matview).
 *
 * Returns number of rows inserted.
 */
static uint64
refresh_matview_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	uint64		processed;

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, queryEnv ? queryEnv: NULL, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	processed = queryDesc->estate->es_processed;

	if (resultTupleDesc)
		*resultTupleDesc = CreateTupleDescCopy(queryDesc->tupDesc);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	return processed;
}

DestReceiver *
CreateTransientRelDestReceiver(Oid transientoid)
{
	DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

	self->pub.receiveSlot = transientrel_receive;
	self->pub.rStartup = transientrel_startup;
	self->pub.rShutdown = transientrel_shutdown;
	self->pub.rDestroy = transientrel_destroy;
	self->pub.mydest = DestTransientRel;
	self->transientoid = transientoid;

	return (DestReceiver *) self;
}

/*
 * transientrel_startup --- executor startup
 */
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	Relation	transientrel;

	transientrel = table_open(myState->transientoid, NoLock);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->transientrel = transientrel;
	myState->output_cid = GetCurrentCommandId(true);
	myState->ti_options = TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN;
	myState->bistate = GetBulkInsertState();

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
static bool
transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	/*
	 * Note that the input slot might not be of the type of the target
	 * relation. That's supported by table_tuple_insert(), but slightly less
	 * efficient than inserting with the right slot - but the alternative
	 * would be to copy into a slot of the right type, which would not be
	 * cheap either. This also doesn't allow accessing per-AM data (say a
	 * tuple's xmin), but since we don't do that here...
	 */

	table_tuple_insert(myState->transientrel,
					   slot,
					   myState->output_cid,
					   myState->ti_options,
					   myState->bistate);

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * transientrel_shutdown --- executor end
 */
static void
transientrel_shutdown(DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	FreeBulkInsertState(myState->bistate);

	table_finish_bulk_insert(myState->transientrel, myState->ti_options);

	/* close transientrel, but keep lock until commit */
	table_close(myState->transientrel, NoLock);
	myState->transientrel = NULL;
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void
transientrel_destroy(DestReceiver *self)
{
	pfree(self);
}


/*
 * Given a qualified temporary table name, append an underscore followed by
 * the given integer, to make a new table name based on the old one.
 * The result is a palloc'd string.
 *
 * As coded, this would fail to make a valid SQL name if the given name were,
 * say, "FOO"."BAR".  Currently, the table name portion of the input will
 * never be double-quoted because it's of the form "pg_temp_NNN", cf
 * make_new_heap().  But we might have to work harder someday.
 */
static char *
make_temptable_name_n(char *tempname, int n)
{
	StringInfoData namebuf;

	initStringInfo(&namebuf);
	appendStringInfoString(&namebuf, tempname);
	appendStringInfo(&namebuf, "_%d", n);
	return namebuf.data;
}

/*
 * refresh_by_match_merge
 *
 * Refresh a materialized view with transactional semantics, while allowing
 * concurrent reads.
 *
 * This is called after a new version of the data has been created in a
 * temporary table.  It performs a full outer join against the old version of
 * the data, producing "diff" results.  This join cannot work if there are any
 * duplicated rows in either the old or new versions, in the sense that every
 * column would compare as equal between the two rows.  It does work correctly
 * in the face of rows which have at least one NULL value, with all non-NULL
 * columns equal.  The behavior of NULLs on equality tests and on UNIQUE
 * indexes turns out to be quite convenient here; the tests we need to make
 * are consistent with default behavior.  If there is at least one UNIQUE
 * index on the materialized view, we have exactly the guarantee we need.
 *
 * The temporary table used to hold the diff results contains just the TID of
 * the old record (if matched) and the ROW from the new table as a single
 * column of complex record type (if matched).
 *
 * Once we have the diff table, we perform set-based DELETE and INSERT
 * operations against the materialized view, and discard both temporary
 * tables.
 *
 * Everything from the generation of the new data to applying the differences
 * takes place under cover of an ExclusiveLock, since it seems as though we
 * would want to prohibit not only concurrent REFRESH operations, but also
 * incremental maintenance.  It also doesn't seem reasonable or safe to allow
 * SELECT FOR UPDATE or SELECT FOR SHARE on rows being updated or deleted by
 * this command.
 */
static void
refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
					   int save_sec_context)
{
	StringInfoData querybuf;
	Relation	matviewRel;
	Relation	tempRel;
	char	   *matviewname;
	char	   *tempname;
	char	   *diffname;
	TupleDesc	tupdesc;
	bool		foundUniqueIndex;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int16		relnatts;
	Oid		   *opUsedForQual;

	initStringInfo(&querybuf);
	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));
	tempRel = table_open(tempOid, NoLock);
	tempname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel)),
										  RelationGetRelationName(tempRel));
	diffname = make_temptable_name_n(tempname, 2);

	relnatts = RelationGetNumberOfAttributes(matviewRel);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Analyze the temp table with the new contents. */
	appendStringInfo(&querybuf, "ANALYZE %s", tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/*
	 * We need to ensure that there are not duplicate rows without NULLs in
	 * the new data set before we can count on the "diff" results.  Check for
	 * that in a way that allows showing the first duplicated row found.  Even
	 * after we pass this test, a unique index on the materialized view may
	 * find a duplicate key problem.
	 *
	 * Note: here and below, we use "tablename.*::tablerowtype" as a hack to
	 * keep ".*" from being expanded into multiple columns in a SELECT list.
	 * Compare ruleutils.c's get_variable().
	 */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "SELECT newdata.*::%s FROM %s newdata "
					 "WHERE newdata.* IS NOT NULL AND EXISTS "
					 "(SELECT 1 FROM %s newdata2 WHERE newdata2.* IS NOT NULL "
					 "AND newdata2.* OPERATOR(pg_catalog.*=) newdata.* "
					 "AND newdata2.ctid OPERATOR(pg_catalog.<>) "
					 "newdata.ctid)",
					 tempname, tempname, tempname);
	if (SPI_execute(querybuf.data, false, 1) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	if (SPI_processed > 0)
	{
		/*
		 * Note that this ereport() is returning data to the user.  Generally,
		 * we would want to make sure that the user has been granted access to
		 * this data.  However, REFRESH MAT VIEW is only able to be run by the
		 * owner of the mat view (or a superuser) and therefore there is no
		 * need to check for access to data in the mat view.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("new data for materialized view \"%s\" contains duplicate rows without any null columns",
						RelationGetRelationName(matviewRel)),
				 errdetail("Row: %s",
						   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))));
	}

	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	/* Start building the query for creating the diff table. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "CREATE TEMP TABLE %s AS "
					 "SELECT mv.ctid AS tid, newdata.*::%s AS newdata "
					 "FROM %s mv FULL JOIN %s newdata ON (",
					 diffname, tempname, matviewname, tempname);

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache.  We will test for equality on all
	 * columns present in all unique indexes which only reference columns and
	 * include all rows.
	 */
	tupdesc = matviewRel->rd_att;
	opUsedForQual = (Oid *) palloc0(sizeof(Oid) * relnatts);
	foundUniqueIndex = false;

	indexoidlist = RelationGetIndexList(matviewRel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indexRel;

		indexRel = index_open(indexoid, RowExclusiveLock);
		if (is_usable_unique_index(indexRel))
		{
			Form_pg_index indexStruct = indexRel->rd_index;
			int			indnkeyatts = indexStruct->indnkeyatts;
			oidvector  *indclass;
			Datum		indclassDatum;
			bool		isnull;
			int			i;

			/* Must get indclass the hard way. */
			indclassDatum = SysCacheGetAttr(INDEXRELID,
											indexRel->rd_indextuple,
											Anum_pg_index_indclass,
											&isnull);
			Assert(!isnull);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			/* Add quals for all columns from this index. */
			for (i = 0; i < indnkeyatts; i++)
			{
				int			attnum = indexStruct->indkey.values[i];
				Oid			opclass = indclass->values[i];
				Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
				Oid			attrtype = attr->atttypid;
				HeapTuple	cla_ht;
				Form_pg_opclass cla_tup;
				Oid			opfamily;
				Oid			opcintype;
				Oid			op;
				const char *leftop;
				const char *rightop;

				/*
				 * Identify the equality operator associated with this index
				 * column.  First we need to look up the column's opclass.
				 */
				cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
				if (!HeapTupleIsValid(cla_ht))
					elog(ERROR, "cache lookup failed for opclass %u", opclass);
				cla_tup = (Form_pg_opclass) GETSTRUCT(cla_ht);
				Assert(cla_tup->opcmethod == BTREE_AM_OID);
				opfamily = cla_tup->opcfamily;
				opcintype = cla_tup->opcintype;
				ReleaseSysCache(cla_ht);

				op = get_opfamily_member(opfamily, opcintype, opcintype,
										 BTEqualStrategyNumber);
				if (!OidIsValid(op))
					elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
						 BTEqualStrategyNumber, opcintype, opcintype, opfamily);

				/*
				 * If we find the same column with the same equality semantics
				 * in more than one index, we only need to emit the equality
				 * clause once.
				 *
				 * Since we only remember the last equality operator, this
				 * code could be fooled into emitting duplicate clauses given
				 * multiple indexes with several different opclasses ... but
				 * that's so unlikely it doesn't seem worth spending extra
				 * code to avoid.
				 */
				if (opUsedForQual[attnum - 1] == op)
					continue;
				opUsedForQual[attnum - 1] = op;

				/*
				 * Actually add the qual, ANDed with any others.
				 */
				if (foundUniqueIndex)
					appendStringInfoString(&querybuf, " AND ");

				leftop = quote_qualified_identifier("newdata",
													NameStr(attr->attname));
				rightop = quote_qualified_identifier("mv",
													 NameStr(attr->attname));

				generate_operator_clause(&querybuf,
										 leftop, attrtype,
										 op,
										 rightop, attrtype);

				foundUniqueIndex = true;
			}
		}

		/* Keep the locks, since we're about to run DML which needs them. */
		index_close(indexRel, NoLock);
	}

	list_free(indexoidlist);

	/*
	 * There must be at least one usable unique index on the matview.
	 *
	 * ExecRefreshMatView() checks that after taking the exclusive lock on the
	 * matview. So at least one unique index is guaranteed to exist here
	 * because the lock is still being held; so an Assert seems sufficient.
	 */
	Assert(foundUniqueIndex);

	appendStringInfoString(&querybuf,
						   " AND newdata.* OPERATOR(pg_catalog.*=) mv.*) "
						   "WHERE newdata.* IS NULL OR mv.* IS NULL "
						   "ORDER BY tid");

	/* Create the temporary "diff" table. */
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	/*
	 * We have no further use for data from the "full-data" temp table, but we
	 * must keep it around because its type is referenced from the diff table.
	 */

	/* Analyze the diff table. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "ANALYZE %s", diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	OpenMatViewIncrementalMaintenance();

	/* Deletes must come before inserts; do them first. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "DELETE FROM %s mv WHERE ctid OPERATOR(pg_catalog.=) ANY "
					 "(SELECT diff.tid FROM %s diff "
					 "WHERE diff.tid IS NOT NULL "
					 "AND diff.newdata IS NULL)",
					 matviewname, diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Inserts go last. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "INSERT INTO %s SELECT (diff.newdata).* "
					 "FROM %s diff WHERE tid IS NULL",
					 matviewname, diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* We're done maintaining the materialized view. */
	CloseMatViewIncrementalMaintenance();
	table_close(tempRel, NoLock);
	table_close(matviewRel, NoLock);

	/* Clean up temp tables. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "DROP TABLE %s, %s", diffname, tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * Swap the physical files of the target and transient tables, then rebuild
 * the target's indexes and throw away the transient table.  Security context
 * swapping is handled by the called function, so it is not needed here.
 */
static void
refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence)
{
	finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
					 RecentXmin, ReadNextMultiXactId(), relpersistence);
}

/*
 * Check whether specified index is usable for match merge.
 */
static bool
is_usable_unique_index(Relation indexRel)
{
	Form_pg_index indexStruct = indexRel->rd_index;

	/*
	 * Must be unique, valid, immediate, non-partial, and be defined over
	 * plain user columns (not expressions).  We also require it to be a
	 * btree.  Even if we had any other unique index kinds, we'd not know how
	 * to identify the corresponding equality operator, nor could we be sure
	 * that the planner could implement the required FULL JOIN with non-btree
	 * operators.
	 */
	if (indexStruct->indisunique &&
		indexStruct->indimmediate &&
		indexRel->rd_rel->relam == BTREE_AM_OID &&
		indexStruct->indisvalid &&
		RelationGetIndexPredicate(indexRel) == NIL &&
		indexStruct->indnatts > 0)
	{
		/*
		 * The point of groveling through the index columns individually is to
		 * reject both index expressions and system columns.  Currently,
		 * matviews couldn't have OID columns so there's no way to create an
		 * index on a system column; but maybe someday that wouldn't be true,
		 * so let's be safe.
		 */
		int			numatts = indexStruct->indnatts;
		int			i;

		for (i = 0; i < numatts; i++)
		{
			int			attnum = indexStruct->indkey.values[i];

			if (attnum <= 0)
				return false;
		}
		return true;
	}
	return false;
}


/*
 * This should be used to test whether the backend is in a context where it is
 * OK to allow DML statements to modify materialized views.  We only want to
 * allow that for internal code driven by the materialized view definition,
 * not for arbitrary user-supplied code.
 *
 * While the function names reflect the fact that their main intended use is
 * incremental maintenance of materialized views (in response to changes to
 * the data in referenced relations), they are initially used to allow REFRESH
 * without blocking concurrent reads.
 */
bool
MatViewIncrementalMaintenanceIsEnabled(void)
{
	return matview_maintenance_depth > 0;
}

static void
OpenMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth++;
}

static void
CloseMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth--;
	Assert(matview_maintenance_depth >= 0);
}

/*
 * get_matview_query - get the Query from a matview's _RETURN rule.
 */
static Query *
get_matview_query(Relation matviewRel)
{
	RewriteRule *rule;
	List * actions;

	/*
	 * Check that everything is correct for a refresh. Problems at this point
	 * are internal errors, so elog is sufficient.
	 */
	if (matviewRel->rd_rel->relhasrules == false ||
		matviewRel->rd_rules->numLocks < 1)
		elog(ERROR,
			 "materialized view \"%s\" is missing rewrite information",
			 RelationGetRelationName(matviewRel));

	if (matviewRel->rd_rules->numLocks > 1)
		elog(ERROR,
			 "materialized view \"%s\" has too many rules",
			 RelationGetRelationName(matviewRel));

	rule = matviewRel->rd_rules->rules[0];
	if (rule->event != CMD_SELECT || !(rule->isInstead))
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
			 RelationGetRelationName(matviewRel));

	actions = rule->actions;
	if (list_length(actions) != 1)
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a single action",
			 RelationGetRelationName(matviewRel));

	/*
	 * The stored query was rewritten at the time of the MV definition, but
	 * has not been scribbled on by the planner.
	 */
	return linitial_node(Query, actions);
}


/* ----------------------------------------------------
 *		Incremental View Maintenance routines
 * ---------------------------------------------------
 */

/*
 * IVM_immediate_before
 *
 * IVM trigger function invoked before base table is modified. If this is
 * invoked firstly in the same statement, we save the transaction id and the
 * command id at that time.
 */
Datum
IVM_immediate_before(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	char	   *matviewOid_text = trigdata->tg_trigger->tgargs[0];
	char	   *ex_lock_text = trigdata->tg_trigger->tgargs[1];
	Oid			matviewOid;
	MV_TriggerHashEntry *entry;
	bool	found;
	bool	ex_lock;

	matviewOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(matviewOid_text)));
	ex_lock = DatumGetBool(DirectFunctionCall1(boolin, CStringGetDatum(ex_lock_text)));

	/* If the view has more than one tables, we have to use an exclusive lock. */
	if (ex_lock)
	{
		/*
		 * Wait for concurrent transactions which update this materialized view at
		 * READ COMMITED. This is needed to see changes committed in other
		 * transactions. No wait and raise an error at REPEATABLE READ or
		 * SERIALIZABLE to prevent update anomalies of matviews.
		 * XXX: dead-lock is possible here.
		 */
		if (!IsolationUsesXactSnapshot())
			LockRelationOid(matviewOid, ExclusiveLock);
		else if (!ConditionalLockRelationOid(matviewOid, ExclusiveLock))
		{
			/* try to throw error by name; relation could be deleted... */
			char	   *relname = get_rel_name(matviewOid);

			if (!relname)
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("could not obtain lock on materialized view during incremental maintenance")));

			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					errmsg("could not obtain lock on materialized view \"%s\" during incremental maintenance",
							relname)));
		}
	}
	else
		LockRelationOid(matviewOid, RowExclusiveLock);

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_trigger_info)
		mv_InitHashTables();

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_ENTER, &found);

	/* On the first BEFORE to update the view, initialize trigger data */
	if (!found)
	{
		/*
		 * Get a snapshot just before the table was modified for checking
		 * tuple visibility in the pre-update state of the table.
		 */
		Snapshot snapshot = GetActiveSnapshot();

		entry->matview_id = matviewOid;
		entry->before_trig_count = 0;
		entry->after_trig_count = 0;
		entry->snapshot = RegisterSnapshot(snapshot);
		entry->tables = NIL;
		entry->has_old = false;
		entry->has_new = false;
	}

	entry->before_trig_count++;

	return PointerGetDatum(NULL);
}

/*
 * IVM_immediate_maintenance
 *
 * IVM trigger function invoked after base table is modified.
 * For each table, tuplestores of transition tables are collected.
 * and after the last modification
 */
Datum
IVM_immediate_maintenance(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel;
	Oid			relid;
	Oid			matviewOid;
	Query	   *query;
	Query	   *rewritten;
	char	   *matviewOid_text = trigdata->tg_trigger->tgargs[0];
	Relation	matviewRel;
	int old_depth = matview_maintenance_depth;

	Oid			relowner;
	Tuplestorestate *old_tuplestore = NULL;
	Tuplestorestate *new_tuplestore = NULL;
	DestReceiver *dest_new = NULL, *dest_old = NULL;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	MV_TriggerHashEntry *entry;
	MV_TriggerTable		*table;
	IvmMaintenanceGraph	*maintenance_graph = NULL;
	bool	hasOuterJoins = false;
	bool	found;

	ParseState		 *pstate;
	QueryEnvironment *queryEnv = create_queryEnv();
	MemoryContext	oldcxt;
	ListCell   *lc;
	int			i;


	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	rel = trigdata->tg_relation;
	relid = rel->rd_id;

	matviewOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(matviewOid_text)));

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_trigger_info)
		mv_InitHashTables();

	/* get the entry for this materialized view */
	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);
	entry->after_trig_count++;

	/* search the entry for the modified table and create new entry if not found */
	found = false;
	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == relid)
		{
			found = true;
			break;
		}
	}
	if (!found)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		table = (MV_TriggerTable *) palloc0(sizeof(MV_TriggerTable));
		table->table_id = relid;
		table->old_tuplestores = NIL;
		table->new_tuplestores = NIL;
		table->old_rtes = NIL;
		table->new_rtes = NIL;
		table->rte_paths = NIL;
		table->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), table_slot_callbacks(rel));
		table->rel = table_open(RelationGetRelid(rel), NoLock);
		entry->tables = lappend(entry->tables, table);

		MemoryContextSwitchTo(oldcxt);
	}

	/* Save the transition tables and make a request to not free immediately */
	if (trigdata->tg_oldtable)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		table->old_tuplestores = lappend(table->old_tuplestores, trigdata->tg_oldtable);
		entry->has_old = true;
		MemoryContextSwitchTo(oldcxt);
	}
	if (trigdata->tg_newtable)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		table->new_tuplestores = lappend(table->new_tuplestores, trigdata->tg_newtable);
		entry->has_new = true;
		MemoryContextSwitchTo(oldcxt);
	}
	if (entry->has_new || entry->has_old)
	{
		CmdType cmd;

		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
			cmd = CMD_INSERT;
		else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
			cmd = CMD_DELETE;
		else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
			cmd = CMD_UPDATE;
		else
			elog(ERROR,"unsupported trigger type");

		/* Prolong lifespan of transition tables to the end of the last AFTER trigger */
		SetTransitionTablePreserved(relid, cmd);
	}


	/* If this is not the last AFTER trigger call, immediately exit. */
	Assert (entry->before_trig_count >= entry->after_trig_count);
	if (entry->before_trig_count != entry->after_trig_count)
		return PointerGetDatum(NULL);

	/*
	 * If this is the last AFTER trigger call, continue and update the view.
	 */

	/*
	 * Advance command counter to make the updated base table row locally
	 * visible.
	 */
	CommandCounterIncrement();

	matviewRel = table_open(matviewOid, NoLock);

	/* Make sure it is a materialized view. */
	Assert(matviewRel->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Get and push the latast snapshot to see any changes which is committed
	 * during waiting in other transactions at READ COMMITTED level.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "refresh a materialized view incrementally");

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also arrange to make GUC variable changes local to this command.
	 * We will switch modes when we are about to execute user code.
	 */
	relowner = matviewRel->rd_rel->relowner;
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/* get view query*/
	query = get_matview_query(matviewRel);

	/* join tree analysis for outer join */
	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_JOIN && IS_OUTER_JOIN(rte->jointype))
		{
			hasOuterJoins = true;
			break;
		}
	}
	if (hasOuterJoins)
		maintenance_graph = make_maintenance_graph(query, matviewRel);

	/*
	 * When a base table is truncated, the view content will be empty if the
	 * view definition query does not contain an outer join or an aggregate
	 * without a GROUP clause. Therefore, such views can be truncated.
	 *
	 * Aggregate views without a GROUP clause always have one row. Therefore,
	 * if a base table is truncated, the view will not be empty and will contain
	 * a row with NULL value (or 0 for count()). So, in this case, we refresh the
	 * view instead of truncating it.
	 *
	 * If you have an outer join, truncating a base table will not empty the
	 * join result. Therefore, refresh the view in this case as well.
	 */
	if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
	{
		if (!hasOuterJoins && !(query->hasAggs && query->groupClause == NIL))
			ExecuteTruncateGuts(list_make1(matviewRel), list_make1_oid(matviewOid),
								NIL, DROP_RESTRICT, false);
		else
		{
			Oid			OIDNewHeap;
			DestReceiver *dest;
			uint64		processed = 0;
			Query	   *dataQuery = rewriteQueryForIMMV(query, NIL);
			char		relpersistence = matviewRel->rd_rel->relpersistence;

			/*
			 * Create the transient table that will receive the regenerated data. Lock
			 * it against access by any other process until commit (by which time it
			 * will be gone).
			 */
			OIDNewHeap = make_new_heap(matviewOid, matviewRel->rd_rel->reltablespace,
									   matviewRel->rd_rel->relam,
									   relpersistence,  ExclusiveLock);
			LockRelationOid(OIDNewHeap, AccessExclusiveLock);
			dest = CreateTransientRelDestReceiver(OIDNewHeap);

			/* Generate the data */
			processed = refresh_matview_datafill(dest, dataQuery, NULL, NULL, "");
			refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

			/* Inform cumulative stats system about our activity */
			pgstat_count_truncate(matviewRel);
			pgstat_count_heap_insert(matviewRel, processed);
		}

		/* Clean up hash entry and delete tuplestores */
		clean_up_IVM_hash_entry(entry, false);

		/* Pop the original snapshot. */
		PopActiveSnapshot();

		table_close(matviewRel, NoLock);

		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		return PointerGetDatum(NULL);
	}

	/*
	 * rewrite query for calculating deltas
	 */

	rewritten = copyObject(query);

	/* Replace resnames in a target list with materialized view's attnames */
	i = 0;
	foreach (lc, rewritten->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		tle->resname = pstrdup(resname);
		i++;
	}

	/* Rewrite for the EXISTS clause */
	if (rewritten->hasSubLinks)
		rewrite_query_for_exists_subquery(rewritten);

	/* Set all tables in the query to pre-update state */
	rewritten = rewrite_query_for_preupdate_state(rewritten, entry->tables,
												  pstate, NIL, matviewOid);
	/* Rewrite for DISTINCT clause and aggregates functions */
	rewritten = rewrite_query_for_distinct_and_aggregates(rewritten, pstate);

	/* Create tuplestores to store view deltas */
	if (entry->has_old)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		old_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_old = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(dest_old,
									old_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);

		MemoryContextSwitchTo(oldcxt);
	}
	if (entry->has_new)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		new_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_new = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(dest_new,
									new_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);
		MemoryContextSwitchTo(oldcxt);
	}

	/* for all modified tables */
	foreach(lc, entry->tables)
	{
		ListCell *lc2;

		table = (MV_TriggerTable *) lfirst(lc);

		/* loop for self-join */
		foreach(lc2, table->rte_paths)
		{
			List *rte_path = lfirst(lc2);
			Query *querytree = rewritten;
			RangeTblEntry  *rte;
			TupleDesc		tupdesc_old;
			TupleDesc		tupdesc_new;
			Query	*query_for_delta;
			bool	in_exists = false;
			bool	use_count = false;
			char   *count_colname = NULL;

			/* check if the modified table is in EXISTS clause. */
			for (int j = 0; j < list_length(rte_path); j++)
			{
				int index =  lfirst_int(list_nth_cell(rte_path, j));
				rte = (RangeTblEntry *) lfirst(list_nth_cell(querytree->rtable, index - 1));

				if (rte != NULL && rte->rtekind == RTE_SUBQUERY)
				{
					querytree = rte->subquery;
					if (rte->lateral)
					{
						int attnum;
						count_colname = getColumnNameStartWith(rte, "__ivm_exists", &attnum);
						if (count_colname)
						{
							use_count = true;
							in_exists = true;
						}
					}
				}
			}

			if (count_colname == NULL && (query->hasAggs || query->distinctClause))
			{
				count_colname = pstrdup("__ivm_count__");
				use_count = true;
			}

			/* For outer join query, we need additional rewrites. */
			if (!in_exists && hasOuterJoins)
			{
				int index;

				Assert(list_length(rte_path) == 1);
				index = linitial_int(rte_path);

				update_maintenance_graph(maintenance_graph, index);
				query_for_delta = rewrite_query_for_outerjoin(rewritten, index, maintenance_graph);
			}
			else
				query_for_delta = rewritten;

			/* calculate delta tables */
			calc_delta(table, rte_path, query_for_delta, dest_old, dest_new,
					   &tupdesc_old, &tupdesc_new, queryEnv);

			/* Set the table in the query to post-update state */
			rewritten = rewrite_query_for_postupdate_state(rewritten, table, rte_path);

			PG_TRY();
			{
				/* apply the delta tables to the materialized view */
				apply_delta(matviewOid, old_tuplestore, new_tuplestore,
							tupdesc_old, tupdesc_new, query, use_count,
							count_colname, hasOuterJoins ? maintenance_graph : NULL);
			}
			PG_CATCH();
			{
				matview_maintenance_depth = old_depth;
				PG_RE_THROW();
			}
			PG_END_TRY();

			/* clear view delta tuplestores */
			if (old_tuplestore)
				tuplestore_clear(old_tuplestore);
			if (new_tuplestore)
				tuplestore_clear(new_tuplestore);
		}
	}

	/* Clean up hash entry and delete tuplestores */
	clean_up_IVM_hash_entry(entry, false);
	if (old_tuplestore)
	{
		dest_old->rDestroy(dest_old);
		tuplestore_end(old_tuplestore);
	}
	if (new_tuplestore)
	{
		dest_new->rDestroy(dest_new);
		tuplestore_end(new_tuplestore);
	}

	/* Pop the original snapshot. */
	PopActiveSnapshot();

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return PointerGetDatum(NULL);
}

/*
 * rewrite_query_for_preupdate_state
 *
 * Rewrite the query so that base tables' RTEs will represent "pre-update"
 * state of tables. This is necessary to calculate view delta after multiple
 * tables are modified.
 */
static Query*
rewrite_query_for_preupdate_state(Query *query, List *tables,
								  ParseState *pstate, List *rte_path, Oid matviewid)
{
	ListCell *lc;
	int num_rte = list_length(query->rtable);
	int i;


	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	/* register delta ENRs only once at first call */
	if (rte_path == NIL)
		register_delta_ENRs(pstate, query, tables);

	/* XXX: Is necessary? Is this right timing? */
	AcquireRewriteLocks(query, true, false);

	/* convert CTEs to subqueries */
	foreach (lc, query->cteList)
	{
		PlannerInfo root;
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (cte->cterefcount == 0)
			continue;

		root.parse = query;
		inline_cte(&root, cte);
	}
	query->cteList = NIL;

	i = 1;
	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);

		/* if rte contains subquery, search recursively */
		if (r->rtekind == RTE_SUBQUERY)
			rewrite_query_for_preupdate_state(r->subquery, tables, pstate, lappend_int(list_copy(rte_path), i), matviewid);
		else
		{
			ListCell *lc2;
			foreach(lc2, tables)
			{
				MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc2);
				/*
				 * if the modified table is found then replace the original RTE with
				 * "pre-state" RTE and append its path to the list.
				 */
				if (r->relid == table->table_id)
				{
					lfirst(lc) = get_prestate_rte(r, table, pstate->p_queryEnv, matviewid);
					table->rte_paths = lappend(table->rte_paths, lappend_int(list_copy(rte_path), i));
					break;
				}
			}
		}

		/* finish the loop if we processed all RTE included in the original query */
		if (i++ >= num_rte)
			break;
	}

	return query;
}

/*
 * register_delta_ENRs
 *
 * For all modified tables, make ENRs for their transition tables
 * and register them to the queryEnv. ENR's RTEs are also appended
 * into the list in query tree.
 */
static void
register_delta_ENRs(ParseState *pstate, Query *query, List *tables)
{
	QueryEnvironment *queryEnv = pstate->p_queryEnv;
	ListCell *lc;
	RangeTblEntry	*rte;

	foreach(lc, tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);
		ListCell *lc2;
		int count;

		count = 0;
		foreach(lc2, table->old_tuplestores)
		{
			Tuplestorestate *oldtable = (Tuplestorestate *) lfirst(lc2);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));
			ParseNamespaceItem *nsitem;

			enr->md.name = make_delta_enr_name("old", table->table_id, count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = NULL;
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(oldtable);
			enr->reldata = oldtable;
			register_ENR(queryEnv, enr);

			nsitem = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			rte = nsitem->p_rte;
			/* if base table has RLS, set security condition to enr */
			rte->securityQuals = get_securityQuals(table->table_id, list_length(query->rtable) + 1, query);

			query->rtable = lappend(query->rtable, rte);
			table->old_rtes = lappend(table->old_rtes, rte);

			count++;
		}

		count = 0;
		foreach(lc2, table->new_tuplestores)
		{
			Tuplestorestate *newtable = (Tuplestorestate *) lfirst(lc2);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));
			ParseNamespaceItem *nsitem;

			enr->md.name = make_delta_enr_name("new", table->table_id, count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = NULL;
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(newtable);
			enr->reldata = newtable;
			register_ENR(queryEnv, enr);

			nsitem = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			rte = nsitem->p_rte;
			/* if base table has RLS, set security condition to enr*/
			rte->securityQuals = get_securityQuals(table->table_id, list_length(query->rtable) + 1, query);

			query->rtable = lappend(query->rtable, rte);
			table->new_rtes = lappend(table->new_rtes, rte);

			count++;
		}
	}
}

#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))
#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))

/*
 * ivm_visible_in_prestate
 *
 * Check visibility of a tuple specified by the tableoid and item pointer
 * using the snapshot taken just before the table was modified.
 */
Datum
ivm_visible_in_prestate(PG_FUNCTION_ARGS)
{
	Oid			tableoid = PG_GETARG_OID(0);
	ItemPointer itemPtr = PG_GETARG_ITEMPOINTER(1);
	Oid			matviewOid = PG_GETARG_OID(2);
	MV_TriggerHashEntry *entry;
	MV_TriggerTable		*table = NULL;
	ListCell   *lc;
	bool	found;
	bool	result;

	if (!in_delta_calculation)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ivm_visible_in_prestate can be called only in delta calculation")));

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);

	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == tableoid)
			break;
	}

	Assert (table != NULL);

	result = table_tuple_fetch_row_version(table->rel, itemPtr, entry->snapshot, table->slot);

	PG_RETURN_BOOL(result);
}

/*
 * get_prestate_rte
 *
 * Rewrite RTE of the modified table to a subquery which represents
 * "pre-state" table. The original RTE is saved in table->rte_original.
 */
static RangeTblEntry*
get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 QueryEnvironment *queryEnv, Oid matviewid)
{
	StringInfoData str;
	RawStmt *raw;
	Query *subquery;
	Relation rel;
	ParseState *pstate;
	char *relname;
	int i;

	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * We can use NoLock here since AcquireRewriteLocks should
	 * have locked the relation already.
	 */
	rel = table_open(table->table_id, NoLock);
	relname = quote_qualified_identifier(
					get_namespace_name(RelationGetNamespace(rel)),
									   RelationGetRelationName(rel));
	table_close(rel, NoLock);

	/*
	 * Filtering inserted row using the snapshot taken before the table
	 * is modified. ctid is required for maintaining outer join views.
	 */
	initStringInfo(&str);
	appendStringInfo(&str,
		"SELECT t.* , ctid::text, tableoid FROM %s t"
		" WHERE ivm_visible_in_prestate(t.tableoid, t.ctid ,%d::oid)",
			relname, matviewid);

	/*
	 * Append deleted rows contained in old transition tables.
	 * Pseudo ctid for outer join views is also added to the ENR using row_number().
	 */
	for (i = 0; i < list_length(table->old_tuplestores); i++)
	{
		appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str," SELECT *, "
			" ((row_number() over())::text || '_' || '%d' || '_' || '%d') AS ctid,"
			" 0 AS tableoid "
			" FROM %s",
			table->table_id, i,
			make_delta_enr_name("old", table->table_id, i));
	}

	/* Get a subquery representing pre-state of the table */
	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
	subquery = transformStmt(pstate, raw->stmt);

	/*
	 * If the query has setOperations, this must be UNION ALL combining ENRs of
	 * old transition tables. In this case, we have to add securityQuals as same
	 * as the original table to ENRs.
	 */
	if (subquery->setOperations != NULL)
	{
		ListCell *lc;

		foreach (lc, subquery->rtable)
		{
			RangeTblEntry *child_rte;
			RangeTblEntry *sub_rte;

			child_rte = (RangeTblEntry *) lfirst(lc);
			Assert(child_rte->subquery != NULL);

			sub_rte = (RangeTblEntry *) linitial(child_rte->subquery->rtable);
			if (sub_rte->rtekind == RTE_NAMEDTUPLESTORE)
				/* rt_index is always 1 bacause subquery must not have any other RTE */
				sub_rte->securityQuals = get_securityQuals(sub_rte->relid, 1, subquery);
		}
	}

	/* save the original RTE */
	table->original_rte = copyObject(rte);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->eref->colnames = lappend(rte->eref->colnames, makeString(pstrdup("ctid")));
	rte->security_barrier = false;
	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

	rte->requiredPerms = 0;		/* no permission check on subquery itself */
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	rte->extraUpdatedCols = NULL;

	return rte;
}

/*
 * make_delta_enr_name
 *
 * Make a name for ENR of a transition table from the base table's oid.
 * prefix will be "new" or "old" depending on its transition table kind..
 */
static char*
make_delta_enr_name(const char *prefix, Oid relid, int count)
{
	char buf[NAMEDATALEN];
	char *name;

	snprintf(buf, NAMEDATALEN, "__ivm_%s_%u_%u", prefix, relid, count);
	name = pstrdup(buf);

	return name;
}

/*
 * union_ENRs
 *
 * Make a single table delta by unionning all transition tables of the modified table
 * whose RTE is specified by
 */
static RangeTblEntry*
union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix,
		   QueryEnvironment *queryEnv)
{
	StringInfoData str;
	ParseState	*pstate;
	RawStmt *raw;
	Query *sub;
	int	i;
	RangeTblEntry *enr_rte;

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	initStringInfo(&str);

	for (i = 0; i < list_length(enr_rtes); i++)
	{
		if (i > 0)
			appendStringInfo(&str, " UNION ALL ");

		/* add pseudo ctid to ENR using row_number */
		appendStringInfo(&str,
			" SELECT *,  "
			" ((row_number() over())::text || '_' || '%d' || '_' || '%d') AS ctid"
			" FROM %s",
			relid, i,
			make_delta_enr_name(prefix, relid, i));
	}

	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
	sub = transformStmt(pstate, raw->stmt);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = sub;
	rte->security_barrier = false;
	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

	rte->requiredPerms = 0;		/* no permission check on subquery itself */
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	rte->extraUpdatedCols = NULL;
	/* if base table has RLS, set security condition to enr*/
	enr_rte = (RangeTblEntry *)linitial(sub->rtable);
	/* rt_index is always 1, bacause subquery has enr_rte only */
	enr_rte->securityQuals = get_securityQuals(relid, 1, sub);

	return rte;
}

/*
 * rewrite_query_for_distinct_and_aggregates
 *
 * Rewrite query for counting DISTINCT clause and aggregate functions.
 */
static Query *
rewrite_query_for_distinct_and_aggregates(Query *query, ParseState *pstate)
{
	TargetEntry *tle_count;
	FuncCall *fn;
	Node *node;
	int varno = 0;
	ListCell *tbl_lc;

	/* For aggregate views */
	if (query->hasAggs)
	{
		ListCell *lc;
		List *aggs = NIL;
		AttrNumber next_resno = list_length(query->targetList) + 1;

		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (IsA(tle->expr, Aggref))
				makeIvmAggColumn(pstate, (Aggref *)tle->expr, tle->resname, &next_resno, &aggs);
		}
		query->targetList = list_concat(query->targetList, aggs);
	}

	/* Add count(*) used for EXISTS clause */
	foreach(tbl_lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(tbl_lc);
		varno++;
		if (rte->subquery)
		{
			char *columnName;
			int attnum;

			/* search ivm_exists_count_X__ column in RangeTblEntry */
			columnName = getColumnNameStartWith(rte, "__ivm_exists", &attnum);
			if (columnName == NULL)
				continue;

			node = (Node *)makeVar(varno ,attnum,
					INT8OID, -1, InvalidOid, 0);

			if (node == NULL)
				continue;
			tle_count = makeTargetEntry((Expr *) node,
										list_length(query->targetList) + 1,
										pstrdup(columnName),
										false);
			query->targetList = lappend(query->targetList, tle_count);
		}
	}

	/* Add count(*) for counting distinct tuples in views */
	fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
	fn->agg_star = true;
	if (!query->groupClause && !query->hasAggs)
		query->groupClause = transformDistinctClause(NULL, &query->targetList, query->sortClause, false);

	node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

	tle_count = makeTargetEntry((Expr *) node,
								list_length(query->targetList) + 1,
								pstrdup("__ivm_count__"),
								false);
	query->targetList = lappend(query->targetList, tle_count);
	query->hasAggs = true;

	return query;
}

/*
 * rewrite_query_for_exists_subquery
 *
 * Rewrite EXISTS sublink in WHERE to LATERAL subquery
 */
static Query *
rewrite_exists_subquery_walker(Query *query, Node *node, int *count)
{
	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				FromExpr *fromexpr;

				/* get subquery in WHERE clause */
				fromexpr = (FromExpr *)query->jointree;
				query = rewrite_exists_subquery_walker(query, fromexpr->quals, count);
				/* drop subquery in WHERE clause */
				if (IsA(fromexpr->quals, SubLink))
					fromexpr->quals = NULL;
				break;
			}
		case T_BoolExpr:
			{
				BoolExprType type;

				type = ((BoolExpr *) node)->boolop;
				switch (type)
				{
					ListCell *lc;
					case AND_EXPR:
						foreach(lc, ((BoolExpr *)node)->args)
						{
							Node *opnode = (Node *)lfirst(lc);
							query = rewrite_exists_subquery_walker(query, opnode, count);
							/* overwrite SubLink node if it is contained in AND_EXPR */
							if (IsA(opnode, SubLink))
								lfirst(lc) = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(true), false, true);
						}
						break;
					case OR_EXPR:
					case NOT_EXPR:
						if (checkExprHasSubLink(node))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("OR or NOT conditions and EXISTS condition are not used together")));
						break;
				}
				break;
			}
		case T_SubLink:
			{
				char aliasName[NAMEDATALEN];
				char columnName[NAMEDATALEN];
				Query *subselect;
				ParseState *pstate;
				RangeTblEntry *rte;
				RangeTblRef *rtr;
				Alias *alias;
				Oid opId;
				ParseNamespaceItem *nsitem;

				TargetEntry *tle_count;
				FuncCall *fn;
				Node *fn_node;
				Expr *opexpr;

				SubLink *sublink = (SubLink *)node;
				/* raise ERROR if there is non-EXISTS sublink */
				if (sublink->subLinkType != EXISTS_SUBLINK)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("this query is not allowed on incrementally maintainable materialized view"),
							 errhint("subquery in WHERE clause only supports subquery with EXISTS clause")));

				subselect = (Query *)sublink->subselect;

				/* raise ERROR if the sublink has CTE */
				if (subselect->cteList)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CTE in EXIST clause is not supported on incrementally maintainable materialized view")));

				pstate = make_parsestate(NULL);
				pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

				/*
				 * convert EXISTS subquery into LATERAL subquery in FROM clause.
				 */

				snprintf(aliasName, sizeof(aliasName), "__ivm_exists_subquery_%d__", *count);
				snprintf(columnName, sizeof(columnName), "__ivm_exists_count_%d__", *count);

				/* add COUNT(*) for counting rows that meet exists condition */
				fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
				fn->agg_star = true;
				fn_node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);
				tle_count = makeTargetEntry((Expr *) fn_node,
											list_length(subselect->targetList) + 1,
											columnName,
											false);
				/* add __ivm_exists_count__ column */
				subselect->targetList = list_concat(subselect->targetList, list_make1(tle_count));
				subselect->hasAggs = true;

				/* add a sub-query whth LATERAL into from clause */
				alias = makeAlias(aliasName, NIL);
				nsitem = addRangeTableEntryForSubquery(pstate, subselect, alias, true, true);
				rte = nsitem->p_rte;
				query->rtable = lappend(query->rtable, rte);

				/* assume the new RTE is at the end */
				rtr = makeNode(RangeTblRef);
				rtr->rtindex = list_length(query->rtable);
				((FromExpr *)query->jointree)->fromlist = lappend(((FromExpr *)query->jointree)->fromlist, rtr);

				/*
				 * EXISTS condition is converted to HAVING count(*) > 0.
				 * We use make_opcllause() to get int84gt( '>' operator). We might be able to use make_op().
				 */
				opId = OpernameGetOprid(list_make1(makeString(">")), INT8OID, INT4OID);
				opexpr = make_opclause(opId, BOOLOID, false,
								(Expr *)fn_node,
								(Expr *)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(0), false, true),
								InvalidOid, InvalidOid);
				fix_opfuncids((Node *) opexpr);
				query->hasSubLinks = false;

				subselect->havingQual = (Node *)opexpr;
				(*count)++;
				break;
			}
		default:
			break;
	}
	return query;
}

Query *
rewrite_query_for_exists_subquery(Query *query)
{
	int count = 0;
	if (query->hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("this query is not allowed on incrementally maintainable materialized view"),
				 errhint("aggregate function and EXISTS condition are not supported at the same time")));

	return rewrite_exists_subquery_walker(query, (Node *)query, &count);
}


/*
 * Comparison function for sorting Terms in normalized form of join tree.
 */
static int
graph_term_cmp(const ListCell *p1, const ListCell *p2)
{
	int	v1 = bms_num_members(((Term *)lfirst(p1))->relids);
	int	v2 = bms_num_members(((Term *)lfirst(p2))->relids);

	if (v1 > v2)
		return -1;
	if (v1 < v2)
		return 1;
	return 0;
}

#define	graph_term_bigger(p1,p2) (graph_term_cmp(p1,p2) < 0)

/*
 * make_maintenance_graph
 *
 * Analyze the join tree and make a view maintenance graph.
 */
static IvmMaintenanceGraph*
make_maintenance_graph(Query *query, Relation matviewRel)
{
	List	*terms = NIL;
	List	*all_qual_vars;
	ListCell *lc1;
	IvmMaintenanceGraph *result;
	int		i;

	result = (IvmMaintenanceGraph *) palloc(sizeof(IvmMaintenanceGraph));
	result->vars_in_quals = NIL;
	result->resnames_in_quals = NIL;

	/*
	 * Transform query's jointree to the normalized form. This is a bag union
	 * of terms each of which is inner join and/or antijoin between base tables.
	 */
	terms = get_normalized_form(query, (Node *)query->jointree);

	/*
	 * The terms list must be sorted in descending order by the number of
	 * nonnullable tables.  This is necessary to determine the order for processing
	 * terms.  The first term is an inner join of all tables and this is the root
	 * node of the maintenance graph.
	 */
	list_sort(terms, graph_term_cmp);
	result->root = linitial(terms);

	/*
	 * Look for parents of each term. Term t2 is a parent of Term t1, if t2 is
	 * a minimal superset of t1, that is, if there does not exists a term t in the
	 * graph such that t is super set of t1 and t2 is super set of t.
	 */
	foreach (lc1, terms)
	{
		Term *t1 = lfirst(lc1);
		ListCell *lc2;

		/* root doesn't have a parent */
		if (t1 == result->root)
			continue;

		foreach (lc2, terms)
		{
			Term *t2 = lfirst(lc2);
			ListCell *lc3;

			/*
			 * If the number of tables in t2 is not bigger than t1, this
			 * can not be a superset. Also, we can finish this loop
			 * because there is no longer a term bigger than t1.
			 */
			if (!graph_term_bigger(lc2,lc1))
				break;

			/* If t2 is superset, this can be a parent. */
			if (bms_is_subset(t1->relids, t2->relids))
			{
				/*
				 * Remove all terms which are supersets of t2 because these are no
				 * longer minimal superset.
				 */
				foreach (lc3, t1->parents)
				{
					Term *t = (Term *) lfirst(lc3);

					if (bms_is_subset(t2->relids, t->relids))
						t1->parents = foreach_delete_current(t1->parents, lc3);
				}
				t1->parents = lappend(t1->parents, t2);
			}
		}

	}
	result->terms = terms;

	/* Get all vars used in quals and their resnames. */
	all_qual_vars = pull_vars_of_level((Node *) result->root->quals, 0);
	i = 0;
	foreach (lc1, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc1);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);
		Var *var;

		i++;

		if (tle->resjunk)
			continue;

		if (!IsA(tle->expr, Var))
			continue;

		var = (Var *)flatten_join_alias_vars(query, (Node *) tle->expr);

		if (list_member(all_qual_vars, var))
		{
			result->vars_in_quals = lappend(result->vars_in_quals, var);
			result->resnames_in_quals = lappend(result->resnames_in_quals, makeString(pstrdup(resname)));
		}
	}

	return result;
}

/*
 * get_normalized_form
 *
 * Transform query's jointree to the normalized form. This is a bag union
 * of terms each of which is inner join and/or antijoin between base tables.
 */
static List*
get_normalized_form(Query *query, Node *jtnode)
{

	if (jtnode == NULL)
		return NULL;

	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;
		Term *term;

		/*
		 * Create and initialize a term for a single table. Currently,
		 * we assume this is a normal table.
		 */
		Assert(rt_fetch(varno, query->rtable)->relkind == RELKIND_RELATION);

		term = (Term*) palloc(sizeof(Term));
		term->relids = bms_make_singleton(varno);
		term->quals = NULL;
		term->parents = NIL;
		term->effect = IVM_NO_EFFECT;

		return  list_make1(term);
	}
	else if (IsA(jtnode, FromExpr))
	{
		List *terms = NIL;
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;
		bool	is_first = true;

		/*
		 * Create a term list using the terms in FROM list. The qual of
		 * WHERE clause is specified only the first step.
		 */
		foreach(l, f->fromlist)
		{
			List *t = get_normalized_form(query, lfirst(l));
			terms = multiply_terms(query, terms, t, (is_first ? f->quals : NULL));
			is_first = false;
		}
		return terms;
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		List	   *lterms = get_normalized_form(query, j->larg),
				   *rterms = get_normalized_form(query, j->rarg);
		List *terms = NIL;

		/* Create a term list from the two term lists and the join qual */
		terms = multiply_terms(query, lterms, rterms, j->quals);

		/* In outer-join cases, add terms for dangling tuples */
		switch (j->jointype)
		{
			case JOIN_LEFT:
				terms = list_concat(terms, lterms);
				break;
			case JOIN_RIGHT:
				terms = list_concat(terms, rterms);
				break;
			case JOIN_FULL:
				terms = list_concat(terms, lterms);
				terms = list_concat(terms, rterms);
				break;
			case JOIN_INNER:
				break;
			default:
				elog(ERROR, "unexpected join type: %d", j->jointype);
		}

		return terms;
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * multiply_terms
 *
 * Create new a term list by multiplying two term lists. If qual is
 * given, remove terms which are filtered by this qual and add this
 * qual to new terms. If one of the two term list is NIL, we return
 * the other after filtering by the qual.
 */
static List*
multiply_terms(Query *query, List *terms1, List *terms2, Node* qual)
{
	List		*result = NIL;
	ListCell	*l1, *l2;
	Relids		qual_relids;
	PlannerInfo root;

	/*
	 * Convert qual to ANDs implicit expression on vars referencing to
	 * the original relations. We assume the qual doesn't contain
	 * non-strict functions.
	 */
	if (qual)
	{
		qual = flatten_join_alias_vars(query, qual);
		qual = eval_const_expressions(NULL, qual);
		qual = (Node *) canonicalize_qual((Expr *) qual, false);
		qual = (Node *) make_ands_implicit((Expr *) qual);
	}
	/* all relids included in qual */
	qual_relids = pull_varnos(&root, qual);

	/* If either is NIL, return the other after filtering by qual. */
	if (terms1 == NIL || terms2 == NIL)
	{
		result = (terms1 == NIL ? terms2 : terms1);
		if (!qual)
			return result;

		foreach (l1, result)
		{
			Term *term = (Term*) lfirst(l1);

			/*
			 * If the relids in the qual are not included, this term can not exist
			 * because this qual references NULL values.
			 * XXX: we assume that the qual is null-rejecting.
			 */
			if (!bms_is_subset(qual_relids, term->relids))
			{
				result = foreach_delete_current(result, l1);
				continue;
			}
			term->quals = lappend(term->quals, qual);
		}

		return result;
	}

	foreach (l1, terms1)
	{
		Term *term1 = (Term*) lfirst(l1);
		foreach (l2, terms2)
		{
			Term *term2 = (Term*) lfirst(l2);

			/*
			 * If the relids in qual are included term1 or term2, the new term joining
			 * these terms will survive under the condition that the qual is null-rejecting.
			 * Otherwise, the new term can not exist because the qual references NULL
			 * values.
			 */
			if (bms_is_subset(qual_relids, bms_union(term1->relids, term2->relids)))
			{
				Term *newterm = (Term*) palloc(sizeof(Term));

				newterm->parents = NIL;
				newterm->effect = IVM_NO_EFFECT;
				newterm->relids = bms_union(term1->relids, term2->relids);
				newterm->quals = list_concat_copy(term1->quals, term2->quals);
				if (qual)
					newterm->quals = lappend(newterm->quals, qual);

				result = lappend(result, newterm);
			}
		}
	}

	return result;
}

/*
 * update_maintenance_graph
 *
 * Update each term's status about effect of a table modifying. Terms including
 * this table is affected directly. Terms whose parents are affected directly is
 * affected indirectly (e.i. dangling tuples on this terms may be inserted or
 * deleted). Other terms are not affected by this modification.
 * index is a range table index of the modified table.
 */
static void
update_maintenance_graph(IvmMaintenanceGraph *graph, int index)
{
	ListCell *lc;

	/* First, mark IVM_DIRECT_EFFECT for terms affected directly. */
	foreach (lc,  graph->terms)
	{
		Term *term = (Term *) lfirst(lc);

		if (bms_is_member(index, term->relids))
			term->effect = IVM_DIRECT_EFFECT;
		else
			term->effect = IVM_NO_EFFECT;
	}
	/*
	 * Then, mark IVM_INDIRECT_EFFECT for terms whose any immediate parent
	 * is affected directly.
	 */
	foreach (lc,  graph->terms)
	{
		Term *t = (Term *) lfirst(lc);
		ListCell *lc2;

		foreach (lc2, t->parents)
		{
			Term *p = (Term *) lfirst(lc2);

			if (t->effect == IVM_NO_EFFECT && p->effect == IVM_DIRECT_EFFECT)
				t->effect = IVM_INDIRECT_EFFECT;
		}
	}
}

/*
 * rewrite_query_for_outerjoin
 *
 * Rewrite query in order to calculate diff table for outer join.
 * index is the relid of the modified table.
 */
static Query*
rewrite_query_for_outerjoin(Query *query, int index, IvmMaintenanceGraph *graph)
{
	Query  *result = copyObject(query);
	int		varno;
	Node   *node;
	TargetEntry *tle;
	List		*args = NIL;
	FuncCall	*fn;
	ParseState	*pstate = make_parsestate(NULL);

	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
	fn->agg_distinct = true;

	/* Rewrite join type for outer join delta */
	if (!rewrite_jointype(result, (Node *)result->jointree, index))
		elog(ERROR, "modified range table %d not found", index);

	/* Add meta information for outer-join delta */
	varno = -1;
	while ((varno = bms_next_member(graph->root->relids, varno)) >= 0)
	{
		Var *var = NULL;
		RangeTblEntry *rte = rt_fetch(varno, result->rtable);

		/* base table */
		if (rte->rtekind == RTE_RELATION)
		{
			var = makeVar(varno, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);
		}
		/* This subquery must be a pre-state table made by get_prestate_rte */
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			ListCell *lc;
			foreach (lc, ((Query *)rte->subquery)->targetList)
			{
				tle = (TargetEntry *) lfirst(lc);
				if (!strcmp(tle->resname, "ctid"))
				{
					var = makeVar(varno, tle->resno, TEXTOID, -1, DEFAULT_COLLATION_OID, 0);
					break;
				}
			}
		}
		else
			elog(ERROR, "unexpected rte kind");

		/*
		 * Use count(distinct ctid) in order to count the tuples of each base tables which
		 * participate in generating a tuple in diff.
		 */
		args = lappend(args, makeConst(INT4OID,
								 -1,
								 InvalidOid,
								 sizeof(int32),
								 Int32GetDatum(varno),
								 false,
								 true));
		node = ParseFuncOrColumn(pstate, fn->funcname, list_make1(var), NULL, fn, false, -1);
		args = lappend(args, node);
	}

	/* Store all the count for each base table in a JSONB object */
	fn = makeFuncCall(list_make1(makeString("json_build_object")), NIL, COERCE_EXPLICIT_CALL, -1);
	node = ParseFuncOrColumn(pstate, fn->funcname, args, NULL, fn, false, -1);
	node = coerce_type(pstate, node, JSONOID, JSONBOID, -1, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
	assign_expr_collations(pstate, node);
	tle = makeTargetEntry((Expr *) node,
							list_length(result->targetList) + 1,
							pstrdup("__ivm_meta__"),
							false);

	result->targetList = lappend(result->targetList, tle);

	return result;
}

/*
 * rewrite_jointype
 *
 * Rewrite jointree in query for calculating the primary delta. index is
 * relid of the modified table. For deletion or insertion on a table,
 * tuples in the primary delta are not nullable on the modified table,
 * so we can convert some outer joins to inner joins, or full outer joins
 * to left/right outer joins depending on the position of the table
 * in the join tree.
 */
static bool
rewrite_jointype(Query *query, Node *node, int index)
{
	if (node == NULL)
		return false;
	if (IsA(node, RangeTblRef))
	{
		if (((RangeTblRef *) node)->rtindex == index)
			return true;
		else
			return false;
	}
	else if (IsA(node, FromExpr))
	{
		FromExpr   *f = (FromExpr *) node;
		ListCell   *l;

		foreach(l, f->fromlist)
		{
			if (rewrite_jointype(query, lfirst(l), index))
				return true;
		}
		return false;
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) node;
		RangeTblEntry *rte = rt_fetch(j->rtindex, query->rtable);

		if (rewrite_jointype(query, j->larg, index))
		{
			if (j->jointype == JOIN_FULL)
				j->jointype = rte->jointype = JOIN_LEFT;
			else if (j->jointype == JOIN_RIGHT)
				j->jointype = rte->jointype = JOIN_INNER;

			return true;
		}
		else if (rewrite_jointype(query, j->rarg, index))
		{
			if (j->jointype == JOIN_FULL)
				j->jointype = rte->jointype = JOIN_RIGHT;
			else if (j->jointype == JOIN_LEFT)
				j->jointype = rte->jointype = JOIN_INNER;
			return true;
		}
		else
			return false;
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(node));
}

/*
 * calc_delta
 *
 * Calculate view deltas generated under the modification of a table specified
 * by the RTE path.
 */
static void
calc_delta(MV_TriggerTable *table, List *rte_path, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new,
			TupleDesc *tupdesc_old, TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv)
{
	ListCell *lc = getRteListCell(query, rte_path);
	RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

	in_delta_calculation = true;

	/* Generate old delta */
	if (list_length(table->old_rtes) > 0)
	{
		/* Replace the modified table with the old delta table and calculate the old view delta. */
		lfirst(lc) = union_ENRs(rte, table->table_id, table->old_rtes, "old", queryEnv);
		refresh_matview_datafill(dest_old, query, queryEnv, tupdesc_old, "");
	}

	/* Generate new delta */
	if (list_length(table->new_rtes) > 0)
	{
		/* Replace the modified table with the new delta table and calculate the new view delta*/
		lfirst(lc) = union_ENRs(rte, table->table_id, table->new_rtes, "new", queryEnv);
		refresh_matview_datafill(dest_new, query, queryEnv, tupdesc_new, "");
	}

	in_delta_calculation = false;
}

/*
 * rewrite_query_for_postupdate_state
 *
 * Rewrite the query so that the specified base table's RTEs will represent
 * "post-update" state of tables. This is called after the view delta
 * calculation due to changes on this table finishes.
 */
static Query*
rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, List *rte_path)
{
	ListCell *lc = getRteListCell(query, rte_path);

	/* Retore the original RTE */
	lfirst(lc) = table->original_rte;

	return query;
}

/*
 * getRteListCell
 *
 * Get ListCell which contains RTE specified by the given path.
 */
static ListCell*
getRteListCell(Query *query, List *rte_path)
{
	ListCell *lc;
	ListCell *rte_lc = NULL;

	Assert(list_length(rte_path) > 0);

	foreach (lc, rte_path)
	{
		int index = lfirst_int(lc);
		RangeTblEntry	*rte;

		rte_lc = list_nth_cell(query->rtable, index - 1);
		rte = (RangeTblEntry *) lfirst(rte_lc);
		if (rte != NULL && rte->rtekind == RTE_SUBQUERY)
			query = rte->subquery;
	}
	return rte_lc;
}

#define IVM_colname(type, col) makeObjectName("__ivm_" type, col, "_")

/*
 * apply_delta
 *
 * Apply deltas to the materialized view. In outer join cases, this requires
 * the view maintenance graph.
 */
static void
apply_delta(Oid matviewOid, Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname, IvmMaintenanceGraph *graph)
{
	StringInfoData querybuf;
	StringInfoData target_list_buf;
	StringInfo	aggs_list_buf = NULL;
	StringInfo	aggs_set_old = NULL;
	StringInfo	aggs_set_new = NULL;
	Relation	matviewRel;
	char	   *matviewname;
	ListCell	*lc;
	int			i;
	List	   *keys = NIL;
	List	   *minmax_list = NIL;
	List	   *is_min_list = NIL;


	/*
	 * get names of the materialized view and delta tables
	 */

	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	/*
	 * Build parts of the maintenance queries
	 */

	initStringInfo(&querybuf);
	initStringInfo(&target_list_buf);

	if (query->hasAggs)
	{
		if (old_tuplestores && tuplestore_tuple_count(old_tuplestores) > 0)
			aggs_set_old = makeStringInfo();
		if (new_tuplestores && tuplestore_tuple_count(new_tuplestores) > 0)
			aggs_set_new = makeStringInfo();
		aggs_list_buf = makeStringInfo();
	}

	/* build string of target list */
	for (i = 0; i < matviewRel->rd_att->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char   *resname = NameStr(attr->attname);

		if (i != 0)
			appendStringInfo(&target_list_buf, ", ");
		appendStringInfo(&target_list_buf, "%s", quote_qualified_identifier(NULL, resname));
	}

	i = 0;
	foreach (lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		i++;

		if (tle->resjunk)
			continue;

		/*
		 * For views without aggregates, all attributes are used as keys to identify a
		 * tuple in a view.
		 */
		if (!query->hasAggs)
			keys = lappend(keys, attr);

		/* For views with aggregates, we need to build SET clause for updating aggregate
		 * values. */
		if (query->hasAggs && IsA(tle->expr, Aggref))
		{
			Aggref *aggref = (Aggref *) tle->expr;
			const char *aggname = get_func_name(aggref->aggfnoid);

			/*
			 * We can use function names here because it is already checked if these
			 * can be used in IMMV by its OID at the definition time.
			 */

			/* count */
			if (!strcmp(aggname, "count"))
				append_set_clause_for_count(resname, aggs_set_old, aggs_set_new, aggs_list_buf);

			/* sum */
			else if (!strcmp(aggname, "sum"))
				append_set_clause_for_sum(resname, aggs_set_old, aggs_set_new, aggs_list_buf);

			/* avg */
			else if (!strcmp(aggname, "avg"))
				append_set_clause_for_avg(resname, aggs_set_old, aggs_set_new, aggs_list_buf,
										  format_type_be(aggref->aggtype));

			/* min/max */
			else if (!strcmp(aggname, "min") || !strcmp(aggname, "max"))
			{
				bool	is_min = (!strcmp(aggname, "min"));

				append_set_clause_for_minmax(resname, aggs_set_old, aggs_set_new, aggs_list_buf, is_min);

				/* make a resname list of min and max aggregates */
				minmax_list = lappend(minmax_list, resname);
				is_min_list = lappend_int(is_min_list, is_min);
			}
			else
				elog(ERROR, "unsupported aggregate function: %s", aggname);
		}
	}

	/* If we have GROUP BY clause, we use its entries as keys. */
	if (query->hasAggs && query->groupClause)
	{
		foreach (lc, query->groupClause)
		{
			SortGroupClause *sgcl = (SortGroupClause *) lfirst(lc);
			TargetEntry		*tle = get_sortgroupclause_tle(sgcl, query->targetList);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);

			keys = lappend(keys, attr);
		}
	}

	/* Start maintaining the materialized view. */
	OpenMatViewIncrementalMaintenance();

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* For tuple deletion */
	if (old_tuplestores && tuplestore_tuple_count(old_tuplestores) > 0)
	{
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		SPITupleTable  *tuptable_recalc = NULL;
		uint64			num_recalc;
		int				rc;

		/* convert tuplestores to ENR, and register for SPI */
		enr->md.name = pstrdup(OLD_DELTA_ENRNAME);
		enr->md.reliddesc = InvalidOid;
		enr->md.tupdesc = tupdesc_old;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(old_tuplestores);
		enr->reldata = old_tuplestores;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI_register failed");

		if (use_count)
			/* apply old delta and get rows to be recalculated */
			apply_old_delta_with_count(matviewname, OLD_DELTA_ENRNAME,
									   keys, aggs_list_buf, aggs_set_old,
									   minmax_list, is_min_list,
									   count_colname, &tuptable_recalc, &num_recalc);
		else
			apply_old_delta(matviewname, OLD_DELTA_ENRNAME, keys);

		/*
		 * If we have min or max, we might have to recalculate aggregate values from base tables
		 * on some tuples. TIDs and keys such tuples are returned as a result of the above query.
		 */
		if (minmax_list && tuptable_recalc)
			recalc_and_set_values(tuptable_recalc, num_recalc, minmax_list, keys, matviewRel);

		/* Insert dangling tuple for outer join views */
		if (graph && !query->hasAggs)
			insert_dangling_tuples(graph, query, matviewRel, OLD_DELTA_ENRNAME, use_count);
	}

	/* For tuple insertion */
	if (new_tuplestores && tuplestore_tuple_count(new_tuplestores) > 0)
	{
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		int rc;

		/* convert tuplestores to ENR, and register for SPI */
		enr->md.name = pstrdup(NEW_DELTA_ENRNAME);
		enr->md.reliddesc = InvalidOid;
		enr->md.tupdesc = tupdesc_new;;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(new_tuplestores);
		enr->reldata = new_tuplestores;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI_register failed");

		/* apply new delta */
		if (use_count)
			apply_new_delta_with_count(matviewname, NEW_DELTA_ENRNAME,
								keys, aggs_set_new, &target_list_buf, count_colname);
		else
			apply_new_delta(matviewname, NEW_DELTA_ENRNAME, &target_list_buf);

		/* Delete dangling tuple for outer join views */
		if (graph && !query->hasAggs)
			delete_dangling_tuples(graph, query, matviewRel, NEW_DELTA_ENRNAME);
	}

	/* We're done maintaining the materialized view. */
	CloseMatViewIncrementalMaintenance();

	table_close(matviewRel, NoLock);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * append_set_clause_for_count
 *
 * Append SET clause string for count aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_count(const char *resname, StringInfo buf_old,
							StringInfo buf_new,StringInfo aggs_list)
{
	/* For tuple deletion */
	if (buf_old)
	{
		/* resname = mv.resname - t.resname */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, resname, "mv", "t", NULL, NULL));
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* resname = mv.resname + diff.resname */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, resname, "mv", "diff", NULL, NULL));
	}

	appendStringInfo(aggs_list, ", %s",
		quote_qualified_identifier("diff", resname)
	);
}

/*
 * append_set_clause_for_sum
 *
 * Append SET clause string for sum aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_sum(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list)
{
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/* sum = mv.sum - t.sum */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, resname, "mv", "t", count_col, NULL)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* sum = mv.sum + diff.sum */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, resname, "mv", "diff", count_col, NULL)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * append_set_clause_for_avg
 *
 * Append SET clause string for avg aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_avg(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list,
						  const char *aggtype)
{
	char *sum_col = IVM_colname("sum", resname);
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/* avg = (mv.sum - t.sum)::aggtype / (mv.count - t.count) */
		appendStringInfo(buf_old,
			", %s = %s OPERATOR(pg_catalog./) %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, aggtype),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
		/* sum = mv.sum - t.sum */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, sum_col),
			get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, NULL)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);

	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* avg = (mv.sum + diff.sum)::aggtype / (mv.count + diff.count) */
		appendStringInfo(buf_new,
			", %s = %s OPERATOR(pg_catalog./) %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, aggtype),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
		/* sum = mv.sum + diff.sum */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, sum_col),
			get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, NULL)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("sum", resname)),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * append_set_clause_for_minmax
 *
 * Append SET clause string for min or max aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 * is_min is true if this is min, false if not.
 */
static void
append_set_clause_for_minmax(const char *resname, StringInfo buf_old,
							 StringInfo buf_new, StringInfo aggs_list,
							 bool is_min)
{
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/*
		 * If the new value doesn't became NULL then use the value remaining
		 * in the view although this will be recomputated afterwords.
		 */
		appendStringInfo(buf_old,
			", %s = CASE WHEN %s THEN NULL ELSE %s END",
			quote_qualified_identifier(NULL, resname),
			get_null_condition_string(IVM_SUB, "mv", "t", count_col),
			quote_qualified_identifier("mv", resname)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/*
		 * min = LEAST(mv.min, diff.min)
		 * max = GREATEST(mv.max, diff.max)
		 */
		appendStringInfo(buf_new,
			", %s = CASE WHEN %s THEN NULL ELSE %s(%s,%s) END",
			quote_qualified_identifier(NULL, resname),
			get_null_condition_string(IVM_ADD, "mv", "diff", count_col),

			is_min ? "LEAST" : "GREATEST",
			quote_qualified_identifier("mv", resname),
			quote_qualified_identifier("diff", resname)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * get_operation_string
 *
 * Build a string to calculate the new aggregate values.
 */
static char *
get_operation_string(IvmOp op, const char *col, const char *arg1, const char *arg2,
					 const char* count_col, const char *castType)
{
	StringInfoData buf;
	StringInfoData castString;
	char   *col1 = quote_qualified_identifier(arg1, col);
	char   *col2 = quote_qualified_identifier(arg2, col);
	char	op_char = (op == IVM_SUB ? '-' : '+');

	initStringInfo(&buf);
	initStringInfo(&castString);

	if (castType)
		appendStringInfo(&castString, "::%s", castType);

	if (!count_col)
	{
		/*
		 * If the attributes don't have count columns then calc the result
		 * by using the operator simply.
		 */
		appendStringInfo(&buf, "(%s OPERATOR(pg_catalog.%c) %s)%s",
			col1, op_char, col2, castString.data);
	}
	else
	{
		/*
		 * If the attributes have count columns then consider the condition
		 * where the result becomes NULL.
		 */
		char *null_cond = get_null_condition_string(op, arg1, arg2, count_col);

		appendStringInfo(&buf,
			"(CASE WHEN %s THEN NULL "
				"WHEN %s IS NULL THEN %s "
				"WHEN %s IS NULL THEN %s "
				"ELSE (%s OPERATOR(pg_catalog.%c) %s)%s END)",
			null_cond,
			col1, col2,
			col2, col1,
			col1, op_char, col2, castString.data
		);
	}

	return buf.data;
}

/*
 * get_null_condition_string
 *
 * Build a predicate string for CASE clause to check if an aggregate value
 * will became NULL after the given operation is applied.
 */
static char *
get_null_condition_string(IvmOp op, const char *arg1, const char *arg2,
						  const char* count_col)
{
	StringInfoData null_cond;
	initStringInfo(&null_cond);

	switch (op)
	{
		case IVM_ADD:
			appendStringInfo(&null_cond,
				"%s OPERATOR(pg_catalog.=) 0 AND %s OPERATOR(pg_catalog.=) 0",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		case IVM_SUB:
			appendStringInfo(&null_cond,
				"%s OPERATOR(pg_catalog.=) %s",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		default:
			elog(ERROR,"unknown operation");
	}

	return null_cond.data;
}


/*
 * apply_old_delta_with_count
 *
 * Execute a query for applying a delta table given by deltname_old
 * which contains tuples to be deleted from to a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct. Also, when a table in EXISTS sub queries
 * is modified.
 *
 * If the view desn't have aggregates or has GROUP BY, this requires a keys
 * list to identify a tuple in the view. If the view has aggregates, this
 * requires strings representing resnames of aggregates and SET clause for
 * updating aggregate values.
 *
 * If the view has min or max aggregate, this requires a list of resnames of
 * min/max aggregates and a list of boolean which represents which entries in
 * minmax_list is min. These are necessary to check if we need to recalculate
 * min or max aggregate values. In this case, this query returns TID and keys
 * of tuples which need to be recalculated.  This result and the number of rows
 * are stored in tuptables and num_recalc repectedly.
 *
 */
static void
apply_old_delta_with_count(const char *matviewname, const char *deltaname_old,
				List *keys, StringInfo aggs_list, StringInfo aggs_set,
				List *minmax_list, List *is_min_list,
				const char *count_colname,
				SPITupleTable **tuptable_recalc, uint64 *num_recalc)
{
	StringInfoData	querybuf;
	char   *match_cond;
	char   *updt_returning = "";
	char   *select_for_recalc = "SELECT";
	bool	agg_without_groupby = (list_length(keys) == 0);

	Assert(tuptable_recalc != NULL);
	Assert(num_recalc != NULL);

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

	/*
	 * We need a special RETURNING clause and SELECT statement for min/max to
	 * check which tuple needs re-calculation from base tables.
	 */
	if (minmax_list)
	{
		updt_returning = get_returning_string(minmax_list, is_min_list, keys);
		select_for_recalc = get_select_for_recalc_string(keys);
	}

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"WITH t AS ("			/* collecting tid of target tuples in the view */
						"SELECT diff.%s, "			/* count column */
								"(diff.%s OPERATOR(pg_catalog.=) mv.%s AND %s) AS for_dlt, "
								"mv.ctid "
								"%s "				/* aggregate columns */
						"FROM %s AS mv, %s AS diff "
						"WHERE %s"					/* tuple matching condition */
					"), updt AS ("			/* update a tuple if this is not to be deleted */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.-) t.%s "
											"%s"	/* SET clauses for aggregates */
						"FROM t WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND NOT for_dlt "
						"%s"						/* RETURNING clause for recalc infomation */
					"), dlt AS ("			/* delete a tuple if this is to be deleted */
						"DELETE FROM %s AS mv USING t "
						"WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND for_dlt"
					") %s",							/* SELECT returning which tuples need to be recalculated */
					count_colname,
					count_colname, count_colname, (agg_without_groupby ? "false" : "true"),
					(aggs_list != NULL ? aggs_list->data : ""),
					matviewname, deltaname_old,
					match_cond,
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					updt_returning,
					matviewname,
					select_for_recalc);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);


	/* Return tuples to be recalculated. */
	if (minmax_list)
	{
		*tuptable_recalc = SPI_tuptable;
		*num_recalc = SPI_processed;
	}
	else
	{
		*tuptable_recalc = NULL;
		*num_recalc = 0;
	}
}

/*
 * apply_old_delta
 *
 * Execute a query for applying a delta table given by deltname_old
 * which contains tuples to be deleted from to a materialized view given by
 * matviewname.  This is used when counting is not required.
 */
static void
apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys)
{
	StringInfoData	querybuf;
	StringInfoData	keysbuf;
	char   *match_cond;
	ListCell *lc;

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

	/* build string of keys list */
	initStringInfo(&keysbuf);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char   *resname = NameStr(attr->attname);
		appendStringInfo(&keysbuf, "%s", quote_qualified_identifier("mv", resname));
		if (lnext(keys, lc))
			appendStringInfo(&keysbuf, ", ");
	}

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
	"DELETE FROM %s WHERE ctid IN ("
		"SELECT tid FROM (SELECT row_number() over (partition by %s) AS \"__ivm_row_number__\","
								  "mv.ctid AS tid,"
								  "diff.\"__ivm_count__\""
						 "FROM %s AS mv, %s AS diff "
						 "WHERE %s) v "
					"WHERE v.\"__ivm_row_number__\" OPERATOR(pg_catalog.<=) v.\"__ivm_count__\")",
					matviewname,
					keysbuf.data,
					matviewname, deltaname_old,
					match_cond);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * apply_new_delta_with_count
 *
 * Execute a query for applying a delta table given by deltname_new
 * which contains tuples to be inserted into a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct. Also, when a table in EXISTS sub queries
 * is modified.
 *
 * If the view desn't have aggregates or has GROUP BY, this requires a keys
 * list to identify a tuple in the view. If the view has aggregates, this
 * requires strings representing SET clause for updating aggregate values.
 */
static void
apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo aggs_set, StringInfo target_list,
				const char* count_colname)
{
	StringInfoData	querybuf;
	StringInfoData	returning_keys;
	ListCell	*lc;
	char	*match_cond = "";

	/* build WHERE condition for searching tuples to be updated */
	match_cond = get_matching_condition_string(keys);

	/* build string of keys list */
	initStringInfo(&returning_keys);
	if (keys)
	{
		foreach (lc, keys)
		{
			Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
			char   *resname = NameStr(attr->attname);
			appendStringInfo(&returning_keys, "%s", quote_qualified_identifier("mv", resname));
			if (lnext(keys, lc))
				appendStringInfo(&returning_keys, ", ");
		}
	}
	else
		appendStringInfo(&returning_keys, "NULL");

	/* Search for matching tuples from the view and update if found or insert if not. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"WITH updt AS ("		/* update a tuple if this exists in the view */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.+) diff.%s "
											"%s "	/* SET clauses for aggregates */
						"FROM %s AS diff "
						"WHERE %s "					/* tuple matching condition */
						"RETURNING %s"				/* returning keys of updated tuples */
					") INSERT INTO %s (%s)"	/* insert a new tuple if this doesn't existw */
						"SELECT %s FROM %s AS diff "
						"WHERE NOT EXISTS (SELECT 1 FROM updt AS mv WHERE %s);",
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					deltaname_new,
					match_cond,
					returning_keys.data,
					matviewname, target_list->data,
					target_list->data, deltaname_new,
					match_cond);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * apply_new_delta
 *
 * Execute a query for applying a delta table given by deltname_new
 * which contains tuples to be inserted into a materialized view given by
 * matviewname.  This is used when counting is not required.
 */
static void
apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list)
{
	StringInfoData	querybuf;

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"INSERT INTO %s (%s) SELECT %s FROM ("
						"SELECT diff.*, generate_series(1, diff.\"__ivm_count__\") "
						"FROM %s AS diff) AS v",
					matviewname, target_list->data, target_list->data,
					deltaname_new);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * get_matching_condition_string
 *
 * Build a predicate string for looking for a tuple with given keys.
 */
static char *
get_matching_condition_string(List *keys)
{
	StringInfoData match_cond;
	ListCell	*lc;

	/* If there is no key columns, the condition is always true. */
	if (keys == NIL)
		return "true";

	initStringInfo(&match_cond);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char   *resname = NameStr(attr->attname);
		char   *mv_resname = quote_qualified_identifier("mv", resname);
		char   *diff_resname = quote_qualified_identifier("diff", resname);
		Oid		typid = attr->atttypid;

		/* Considering NULL values, we can not use simple = operator. */
		appendStringInfo(&match_cond, "(");
		generate_equal(&match_cond, typid, mv_resname, diff_resname);
		appendStringInfo(&match_cond, " OR (%s IS NULL AND %s IS NULL))",
						 mv_resname, diff_resname);

		if (lnext(keys, lc))
			appendStringInfo(&match_cond, " AND ");
	}

	return match_cond.data;
}

/*
 * get_returning_string
 *
 * Build a string for RETURNING clause of UPDATE used in apply_old_delta_with_count.
 * This clause returns ctid and a boolean value that indicates if we need to
 * recalculate min or max value, for each updated row.
 */
static char *
get_returning_string(List *minmax_list, List *is_min_list, List *keys)
{
	StringInfoData returning;
	char		*recalc_cond;
	ListCell	*lc;

	Assert(minmax_list != NIL && is_min_list != NIL);
	recalc_cond = get_minmax_recalc_condition_string(minmax_list, is_min_list);

	initStringInfo(&returning);

	appendStringInfo(&returning, "RETURNING mv.ctid AS tid, (%s) AS recalc", recalc_cond);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char *resname = NameStr(attr->attname);
		appendStringInfo(&returning, ", %s", quote_qualified_identifier("mv", resname));
	}

	return returning.data;
}

/*
 * get_minmax_recalc_condition_string
 *
 * Build a predicate string for checking if any min/max aggregate
 * value needs to be recalculated.
 */
static char *
get_minmax_recalc_condition_string(List *minmax_list, List *is_min_list)
{
	StringInfoData recalc_cond;
	ListCell	*lc1, *lc2;

	initStringInfo(&recalc_cond);

	Assert (list_length(minmax_list) == list_length(is_min_list));

	forboth (lc1, minmax_list, lc2, is_min_list)
	{
		char   *resname = (char *) lfirst(lc1);
		bool	is_min = (bool) lfirst_int(lc2);
		char   *op_str = (is_min ? ">=" : "<=");

		appendStringInfo(&recalc_cond, "%s OPERATOR(pg_catalog.%s) %s",
			quote_qualified_identifier("mv", resname),
			op_str,
			quote_qualified_identifier("t", resname)
		);

		if (lnext(minmax_list, lc1))
			appendStringInfo(&recalc_cond, " OR ");
	}

	return recalc_cond.data;
}

/*
 * get_select_for_recalc_string
 *
 * Build a query to return tid and keys of tuples which need
 * recalculation. This is used as the result of the query
 * built by apply_old_delta.
 */
static char *
get_select_for_recalc_string(List *keys)
{
	StringInfoData qry;
	ListCell	*lc;

	initStringInfo(&qry);

	appendStringInfo(&qry, "SELECT tid");
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		appendStringInfo(&qry, ", %s", NameStr(attr->attname));
	}

	appendStringInfo(&qry, " FROM updt WHERE recalc");

	return qry.data;
}

/*
 * recalc_and_set_values
 *
 * Recalculate tuples in a materialized from base tables and update these.
 * The tuples which needs recalculation are specified by keys, and resnames
 * of columns to be updated are specified by namelist. TIDs and key values
 * are given by tuples in tuptable_recalc. Its first attribute must be TID
 * and key values must be following this.
 */
static void
recalc_and_set_values(SPITupleTable *tuptable_recalc, int64 num_tuples,
					  List *namelist, List *keys, Relation matviewRel)
{
	TupleDesc   tupdesc_recalc = tuptable_recalc->tupdesc;
	Oid		   *keyTypes = NULL, *types = NULL;
	char	   *keyNulls = NULL, *nulls = NULL;
	Datum	   *keyVals = NULL, *vals = NULL;
	int			num_vals = list_length(namelist);
	int			num_keys = list_length(keys);
	uint64      i;
	Oid			matviewOid;
	char	   *matviewname;

	matviewOid = RelationGetRelid(matviewRel);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	/* If we have keys, initialize arrays for them. */
	if (keys)
	{
		keyTypes = palloc(sizeof(Oid) * num_keys);
		keyNulls = palloc(sizeof(char) * num_keys);
		keyVals = palloc(sizeof(Datum) * num_keys);
		/* a tuple contains keys to be recalculated and ctid to be updated*/
		Assert(tupdesc_recalc->natts == num_keys + 1);

		/* Types of key attributes  */
		for (i = 0; i < num_keys; i++)
			keyTypes[i] = TupleDescAttr(tupdesc_recalc, i + 1)->atttypid;
	}

	/* allocate memory for all attribute names and tid */
	types = palloc(sizeof(Oid) * (num_vals + 1));
	nulls = palloc(sizeof(char) * (num_vals + 1));
	vals = palloc(sizeof(Datum) * (num_vals + 1));

	/* For each tuple which needs recalculation */
	for (i = 0; i < num_tuples; i++)
	{
		int j;
		bool isnull;
		SPIPlanPtr plan;
		SPITupleTable *tuptable_newvals;
		TupleDesc   tupdesc_newvals;

		/* Set group key values as parameters if needed. */
		if (keys)
		{
			for (j = 0; j < num_keys; j++)
			{
				keyVals[j] = SPI_getbinval(tuptable_recalc->vals[i], tupdesc_recalc, j + 2, &isnull);
				if (isnull)
					keyNulls[j] = 'n';
				else
					keyNulls[j] = ' ';
			}
		}

		/*
		 * Get recalculated values from base tables. The result must be
		 * only one tuple thich contains the new values for specified keys.
		 */
		plan = get_plan_for_recalc(matviewOid, namelist, keys, keyTypes);
		if (SPI_execute_plan(plan, keyVals, keyNulls, false, 0) != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_plan");
		if (SPI_processed != 1)
			elog(ERROR, "SPI_execute_plan returned zero or more than one rows");

		tuptable_newvals = SPI_tuptable;
		tupdesc_newvals = tuptable_newvals->tupdesc;

		Assert(tupdesc_newvals->natts == num_vals);

		/* Set the new values as parameters */
		for (j = 0; j < tupdesc_newvals->natts; j++)
		{
			if (i == 0)
				types[j] = TupleDescAttr(tupdesc_newvals, j)->atttypid;

			vals[j] = SPI_getbinval(tuptable_newvals->vals[0], tupdesc_newvals, j + 1, &isnull);
			if (isnull)
				nulls[j] = 'n';
			else
				nulls[j] = ' ';
		}
		/* Set TID of the view tuple to be updated as a parameter */
		types[j] = TIDOID;
		vals[j] = SPI_getbinval(tuptable_recalc->vals[i], tupdesc_recalc, 1, &isnull);
		nulls[j] = ' ';

		/* Update the view tuple to the new values */
		plan = get_plan_for_set_values(matviewOid, matviewname, namelist, types);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) != SPI_OK_UPDATE)
			elog(ERROR, "SPI_execute_plan");
	}
}


/*
 * get_plan_for_recalc
 *
 * Create or fetch a plan for recalculating value in the view's target list
 * from base tables using the definition query of materialized view specified
 * by matviewOid. namelist is a list of resnames of values to be recalculated.
 *
 * keys is a list of keys to identify tuples to be recalculated if this is not
 * empty. KeyTypes is an array of types of keys.
 */
static SPIPlanPtr
get_plan_for_recalc(Oid matviewOid, List *namelist, List *keys, Oid *keyTypes)
{
	MV_QueryKey hash_key;
	SPIPlanPtr	plan;

	/* Fetch or prepare a saved plan for the recalculation */
	mv_BuildQueryKey(&hash_key, matviewOid, MV_PLAN_RECALC);
	if ((plan = mv_FetchPreparedPlan(&hash_key)) == NULL)
	{
		ListCell	   *lc;
		StringInfoData	str;
		char   *viewdef;

		/* get view definition of matview */
		viewdef = text_to_cstring((text *) DatumGetPointer(
					DirectFunctionCall1(pg_get_viewdef, ObjectIdGetDatum(matviewOid))));
		/* get rid of trailing semi-colon */
		viewdef[strlen(viewdef)-1] = '\0';

		/*
		 * Build a query string for recalculating values. This is like
		 *
		 *  SELECT x1, x2, x3, ... FROM ( ... view definition query ...) mv
		 *   WHERE (key1, key2, ...) = ($1, $2, ...);
		 */

		initStringInfo(&str);
		appendStringInfo(&str, "SELECT ");
		foreach (lc, namelist)
		{
			appendStringInfo(&str, "%s", (char *) lfirst(lc));
			if (lnext(namelist, lc))
				appendStringInfoString(&str, ", ");
		}
		appendStringInfo(&str, " FROM (%s) mv", viewdef);

		if (keys)
		{
			int		i = 1;
			char	paramname[16];

			appendStringInfo(&str, " WHERE (");
			foreach (lc, keys)
			{
				Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
				char   *resname = NameStr(attr->attname);
				Oid		typid = attr->atttypid;

				sprintf(paramname, "$%d", i);
				appendStringInfo(&str, "(");
				generate_equal(&str, typid, resname, paramname);
				appendStringInfo(&str, " OR (%s IS NULL AND %s IS NULL))",
								 resname, paramname);

				if (lnext(keys, lc))
					appendStringInfoString(&str, " AND ");
				i++;
			}
			appendStringInfo(&str, ")");
		}
		else
			keyTypes = NULL;

		plan = SPI_prepare(str.data, list_length(keys), keyTypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), str.data);

		SPI_keepplan(plan);
		mv_HashPreparedPlan(&hash_key, plan);
	}

	return plan;
}

/*
 * get_plan_for_set_values
 *
 * Create or fetch a plan for applying new values calculated by
 * get_plan_for_recalc to a materialized view specified by matviewOid.
 * matviewname is the name of the view.  namelist is a list of resnames
 * of attributes to be updated, and valTypes is an array of types of the
 * values.
 */
static SPIPlanPtr
get_plan_for_set_values(Oid matviewOid, char *matviewname, List *namelist,
						Oid *valTypes)
{
	MV_QueryKey	key;
	SPIPlanPtr	plan;

	/* Fetch or prepare a saved plan for the real check */
	mv_BuildQueryKey(&key, matviewOid, MV_PLAN_SET_VALUE);
	if ((plan = mv_FetchPreparedPlan(&key)) == NULL)
	{
		ListCell	  *lc;
		StringInfoData str;
		int		i;

		/*
		 * Build a query string for applying min/max values. This is like
		 *
		 *  UPDATE matviewname AS mv
		 *   SET (x1, x2, x3, x4) = ($1, $2, $3, $4)
		 *   WHERE ctid = $5;
		 */

		initStringInfo(&str);
		appendStringInfo(&str, "UPDATE %s AS mv SET (", matviewname);
		foreach (lc, namelist)
		{
			appendStringInfo(&str, "%s", (char *) lfirst(lc));
			if (lnext(namelist, lc))
				appendStringInfoString(&str, ", ");
		}
		appendStringInfo(&str, ") = ROW(");

		for (i = 1; i <= list_length(namelist); i++)
			appendStringInfo(&str, "%s$%d", (i==1 ? "" : ", "), i);

		appendStringInfo(&str, ") WHERE ctid OPERATOR(pg_catalog.=) $%d", i);

		plan = SPI_prepare(str.data, list_length(namelist) + 1, valTypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), str.data);

		SPI_keepplan(plan);
		mv_HashPreparedPlan(&key, plan);
	}

	return plan;
}

/*
 * insert_dangling_tuples
 *
 * Insert dangling tuples generated as a result of tuples deletion
 * on base tables of a materialized view which has outer join.
 * graph is the view maintenance graph.
 */
static void
insert_dangling_tuples(IvmMaintenanceGraph *graph, Query *query,
					   Relation matviewRel, const char *deltaname_old,
					   bool use_count)
{
	StringInfoData querybuf;
	char	   *matviewname;
	ListCell   *lc;

	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	foreach (lc, graph->terms)
	{
		ListCell	*lc1, *lc2, *lc_p;
		StringInfoData exists_cond;
		StringInfoData targetlist;
		StringInfoData parents_cond;
		StringInfoData count;
		Term *term = lfirst(lc);
		char *sep = "";
		char *sep2 = "";
		int		i;

		if (term->effect != IVM_INDIRECT_EFFECT)
			continue;

		initStringInfo(&exists_cond);
		initStringInfo(&targetlist);
		initStringInfo(&parents_cond);
		initStringInfo(&count);

		i = 0;
		foreach (lc1, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc1);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
			char   *resname = NameStr(attr->attname);
			char   *mv_resname = quote_qualified_identifier("mv", resname);
			char   *diff_resname = quote_qualified_identifier("diff", resname);
			Oid		typid = attr->atttypid;
			Relids	tle_relids;
			PlannerInfo root;

			i++;

			if (tle->resjunk)
				continue;

			tle = (TargetEntry *) flatten_join_alias_vars(query, (Node *) tle);

			/* get relids referenced in this entry */
			tle_relids = pull_varnos_of_level(&root, (Node *)tle, 0);

			/*
			 * If all the column under this entry are belonging the nonnullable table, the value
			 * in the diff can be used in dangling tuples inserted into the view. Otherwise, NULL
			 * is used as the value of the entry because it is assumed that the entry expression
			 * doesn't contain any non-strict function.
			 */
			if (bms_is_subset(tle_relids, term->relids))
			{
				appendStringInfo(&targetlist, "%s%s", sep2, resname);
				sep2 = ",";

				/* Use qual vars to check if this tuple still exists in the view */
				if (IsA(tle->expr, Var) &&
					list_member(graph->vars_in_quals, (Var*) tle->expr))
				{
					appendStringInfo(&exists_cond, " %s ", sep);
					generate_equal(&exists_cond, typid,  mv_resname, diff_resname);
					sep = "AND";
				}
			}
		}

		/*
		 * Looking for counting columns for EXISTS clauses
		 *
		 * XXX: Currently subqueries can not be used with outer joins, so
		 * we arise an error here. However, when we support this in future,
		 * these columns have to be added into the targetlist.
		 */
		for (; i < matviewRel->rd_att->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
			char *resname = NameStr(attr->attname);
			if (!strncmp(resname, "__ivm_exists", 12))
				elog(ERROR, "EXISTS cannot be used with outer joins");
		}

		/* counting the number of tuples to be inserted */
		sep = "";
		i = -1;
		while ((i = bms_next_member(term->relids, i)) >= 0)
		{
			appendStringInfo(&count, "%s (__ivm_meta__->'%d')::pg_catalog.int8", sep, i);
			sep = " OPERATOR(pg_catalog.*) ";
		}

		sep = "";
		/* Build a condition for tuples belonging to any directly affected parent terms */
		foreach (lc_p, term->parents)
		{
			Term *p = (Term *) lfirst(lc_p);

			if (p->effect != IVM_DIRECT_EFFECT)
				continue;

			appendStringInfo(&parents_cond, "%s", sep);

			sep2 = "";
			forboth (lc1, graph->vars_in_quals, lc2, graph->resnames_in_quals)
			{
				Var *var = (Var *) lfirst(lc1);
				char *resname = strVal(lfirst(lc2));

				if (bms_is_member(var->varno, p->relids))
				{
					appendStringInfo(&parents_cond, "%s %s IS NOT NULL ", sep2, resname);
					sep2 = "AND";
				}
			}
			sep = "OR ";
		}

		/* Insert dangling tuples if needed */
		initStringInfo(&querybuf);
		if (use_count)
			appendStringInfo(&querybuf,
				"INSERT INTO %s (%s, __ivm_count__) "
					"SELECT diff.* FROM "
						"(SELECT DISTINCT %s, %s AS __ivm_count__ FROM %s "
						"WHERE %s ) AS diff "
					"WHERE NOT EXISTS (SELECT 1 FROM %s mv WHERE %s)",
				matviewname, targetlist.data,
				targetlist.data, count.data, deltaname_old,
				parents_cond.data,
				matviewname, exists_cond.data
			);
		else
			appendStringInfo(&querybuf,
				"INSERT INTO %s (%s) "
					"SELECT %s FROM "
						"(SELECT diff.*, generate_series(1, diff.__ivm_count__) "
						"FROM (SELECT DISTINCT %s, %s AS __ivm_count__ FROM %s "
						"WHERE %s ) AS diff "
					"WHERE NOT EXISTS (SELECT 1 FROM %s mv WHERE %s)) v",
				matviewname, targetlist.data,
				targetlist.data, targetlist.data, count.data, deltaname_old,
				parents_cond.data,
				matviewname, exists_cond.data
			);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}
}

/*
 * delete_dangling_tuples
 *
 * Delete dangling tuples generated as a result of tuples insertion
 * on base tables of a materialized view which has outer join.
 * graph is the view maintenance graph.
 */
static void
delete_dangling_tuples(IvmMaintenanceGraph *graph, Query *query,
					   Relation matviewRel, const char *deltaname_new)
{
	char	 *matviewname;
	ListCell *lc;

	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	foreach (lc, graph->terms)
	{
		ListCell	*lc1, *lc2, *lc_p;
		StringInfoData querybuf;
		StringInfoData dangling_cond;
		StringInfoData key_cols;
		StringInfoData parents_cond;
		Term *term = lfirst(lc);
		char *sep = "";
		char *sep2 = "";

		if (term->effect != IVM_INDIRECT_EFFECT)
			continue;

		initStringInfo(&dangling_cond);
		initStringInfo(&key_cols);
		initStringInfo(&parents_cond);

		/* Build a condition for looking up all dangling tuples in indirectly affected term */
		forboth (lc1, graph->vars_in_quals, lc2, graph->resnames_in_quals)
		{
			Var *var = (Var *) lfirst(lc1);
			char *resname = strVal(lfirst(lc2));

			if (bms_is_member(var->varno, term->relids))
			{
				appendStringInfo(&dangling_cond, "%s %s IS NOT NULL ", sep, resname);
				appendStringInfo(&key_cols, "%s%s", sep2, resname);
				sep2 = ",";
			}
			else
				appendStringInfo(&dangling_cond, "%s %s IS NULL ", sep, resname);

			sep = "AND";
		}

		sep = "";
		/* Build a condition for tuples belonging to any directly affected parent terms */
		foreach (lc_p, term->parents)
		{
			Term *p = (Term *) lfirst(lc_p);

			if (p->effect != IVM_DIRECT_EFFECT)
				continue;

			appendStringInfo(&parents_cond, "%s", sep);

			sep2 = "";
			forboth (lc1, graph->vars_in_quals, lc2, graph->resnames_in_quals)
			{
				Var *var = (Var *) lfirst(lc1);
				char *resname = strVal(lfirst(lc2));

				if (bms_is_member(var->varno, p->relids))
				{
					appendStringInfo(&parents_cond, "%s %s IS NOT NULL ", sep2, resname);
					sep2 = "AND";
				}
			}
			sep = "OR ";
		}

		/* Delete dangling tuples if needed */
		initStringInfo(&querybuf);
		appendStringInfo(&querybuf,
			"DELETE FROM %s "
			"WHERE %s AND "
				"(%s) IN (SELECT %s FROM %s diff WHERE %s)",
			matviewname,
			dangling_cond.data,
			key_cols.data, key_cols.data, deltaname_new, parents_cond.data
		);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}
}

/*
 * generate_equals
 *
 * Generate an equality clause using given operands' default equality
 * operator.
 */
static void
generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop)
{
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(opttype, TYPECACHE_EQ_OPR);
	if (!OidIsValid(typentry->eq_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an equality operator for type %s",
						format_type_be(opttype))));

	generate_operator_clause(querybuf,
							 leftop, opttype,
							 typentry->eq_opr,
							 rightop, opttype);
}

/*
 * mv_InitHashTables
 */
static void
mv_InitHashTables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MV_QueryKey);
	ctl.entrysize = sizeof(MV_QueryHashEntry);
	mv_query_cache = hash_create("MV query cache",
								 MV_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(MV_TriggerHashEntry);
	mv_trigger_info = hash_create("MV trigger info",
								 MV_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * mv_FetchPreparedPlan
 */
static SPIPlanPtr
mv_FetchPreparedPlan(MV_QueryKey *key)
{
	MV_QueryHashEntry *entry;
	SPIPlanPtr	plan;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_query_cache)
		mv_InitHashTables();

	/*
	 * Lookup for the key
	 */
	entry = (MV_QueryHashEntry *) hash_search(mv_query_cache,
											  (void *) key,
											  HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/*
	 * Check whether the plan is still valid.  If it isn't, we don't want to
	 * simply rely on plancache.c to regenerate it; rather we should start
	 * from scratch and rebuild the query text too.  This is to cover cases
	 * such as table/column renames.  We depend on the plancache machinery to
	 * detect possible invalidations, though.
	 *
	 * CAUTION: this check is only trustworthy if the caller has already
	 * locked both materialized views and base tables.
	 */
	plan = entry->plan;
	if (plan && SPI_plan_is_valid(plan))
		return plan;

	/*
	 * Otherwise we might as well flush the cached plan now, to free a little
	 * memory space before we make a new one.
	 */
	entry->plan = NULL;
	if (plan)
		SPI_freeplan(plan);

	return NULL;
}

/*
 * mv_HashPreparedPlan
 *
 * Add another plan to our private SPI query plan hashtable.
 */
static void
mv_HashPreparedPlan(MV_QueryKey *key, SPIPlanPtr plan)
{
	MV_QueryHashEntry *entry;
	bool		found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_query_cache)
		mv_InitHashTables();

	/*
	 * Add the new plan.  We might be overwriting an entry previously found
	 * invalid by mv_FetchPreparedPlan.
	 */
	entry = (MV_QueryHashEntry *) hash_search(mv_query_cache,
											  (void *) key,
											  HASH_ENTER, &found);
	Assert(!found || entry->plan == NULL);
	entry->plan = plan;
}

/*
 * mv_BuildQueryKey
 *
 * Construct a hashtable key for a prepared SPI plan for IVM.
 */
static void
mv_BuildQueryKey(MV_QueryKey *key, Oid matview_id, int32 query_type)
{
	/*
	 * We assume struct MV_QueryKey contains no padding bytes, else we'd need
	 * to use memset to clear them.
	 */
	key->matview_id = matview_id;
	key->query_type = query_type;
}

/*
 * AtAbort_IVM
 *
 * Clean up hash entries for all materialized views. This is called at
 * transaction abort.
 */
void
AtAbort_IVM()
{
	HASH_SEQ_STATUS seq;
	MV_TriggerHashEntry *entry;

	if (mv_trigger_info)
	{
		hash_seq_init(&seq, mv_trigger_info);
		while ((entry = hash_seq_search(&seq)) != NULL)
			clean_up_IVM_hash_entry(entry, true);
	}

	in_delta_calculation = false;
}

/*
 * clean_up_IVM_hash_entry
 *
 * Clean up tuple stores and hash entries for a materialized view after its
 * maintenance finished.
 */
static void
clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry, bool is_abort)
{
	bool found;
	ListCell *lc;

	foreach(lc, entry->tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);

		list_free(table->old_tuplestores);
		list_free(table->new_tuplestores);
		if (!is_abort)
		{
			ExecDropSingleTupleTableSlot(table->slot);
			table_close(table->rel, NoLock);
		}
	}
	list_free(entry->tables);

	if (!is_abort)
		UnregisterSnapshot(entry->snapshot);

	hash_search(mv_trigger_info, (void *) &entry->matview_id, HASH_REMOVE, &found);
}

/*
 * getColumnNameStartWith
 *
 * Search a column name which starts with the given string from the given RTE,
 * and return the first found one or NULL if not found.
 */
char *
getColumnNameStartWith(RangeTblEntry *rte, char *str, int *attnum)
{
	char *colname;
	ListCell *lc;
	Alias *alias = rte->eref;

	(*attnum) = 0;
	foreach(lc, alias->colnames)
	{
		(*attnum)++;
		if (strncmp(strVal(lfirst(lc)), str, strlen(str)) == 0)
		{
			colname = pstrdup(strVal(lfirst(lc)));
			return colname;
		}
	}
	return NULL;
}

/*
 * isIvmName
 *
 * Check if this is a IVM hidden column from the name.
 */
bool
isIvmName(const char *s)
{
	if (s)
		return (strncmp(s, "__ivm_", 6) == 0);
	return false;
}

/*
 * get_securityQuals
 *
 * Get row security policy on a relation.
 * This is used by IVM for copying RLS from base table to enr.
 */
static List *
get_securityQuals(Oid relId, int rt_index, Query *query)
{
	ParseState *pstate;
	Relation rel;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *rte;
	List *securityQuals;
	List *withCheckOptions;
	bool  hasRowSecurity;
	bool  hasSubLinks;

	securityQuals = NIL;
	pstate = make_parsestate(NULL);

	rel = table_open(relId, NoLock);
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);
	rte = nsitem->p_rte;

	get_row_security_policies(query, rte, rt_index,
							  &securityQuals, &withCheckOptions,
							  &hasRowSecurity, &hasSubLinks);

	/*
	 * Make sure the query is marked correctly if row level security
	 * applies, or if the new quals had sublinks.
	 */
	if (hasRowSecurity)
		query->hasRowSecurity = true;
	if (hasSubLinks)
		query->hasSubLinks = true;

	table_close(rel, NoLock);

	return securityQuals;
}

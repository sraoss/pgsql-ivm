/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  materialized view support
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "commands/cluster.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/createas.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "utils/regproc.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "nodes/print.h"
#include "catalog/pg_type_d.h"
#include "optimizer/optimizer.h"
#include "commands/defrem.h"

/*
 * Local definitions
 */

#define MV_INIT_QUERYHASHSIZE	16

/* MV query type codes */
#define MV_PLAN_RECALC_MINMAX	1
#define MV_PLAN_SET_MINMAX		2

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
 */
typedef struct MV_QueryHashEntry
{
	MV_QueryKey key;
	SPIPlanPtr	plan;
} MV_QueryHashEntry;

/*
 * MV_TriggerHashEntry
 */
typedef struct MV_TriggerHashEntry
{
	Oid	matview_id;
	int	before_trig_count;
	int	after_trig_count;
	TransactionId	xid;
	CommandId	cid;
	List *tables;
	bool	has_old;
	bool	has_new;
} MV_TriggerHashEntry;

/*
 * MV_TriggerTable
 */
typedef struct MV_TriggerTable
{
	Oid	table_id;
	List *old_tuplestores;
	List *new_tuplestores;
	RangeTblEntry *original_rte;
	List *old_rtes;
	List *new_rtes;
	List *rte_indexes;
} MV_TriggerTable;

static HTAB *mv_query_cache = NULL;
static HTAB *mv_trigger_info = NULL;

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

typedef enum
{
	IVM_ADD,
	IVM_SUB
} IvmOp;

static int	matview_maintenance_depth = 0;

static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);
static uint64 refresh_matview_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 const char *queryString);
static char *make_temptable_name_n(char *tempname, int n);
static void refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
								   int save_sec_context);
static void refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence);
static bool is_usable_unique_index(Relation indexRel);
static void OpenMatViewIncrementalMaintenance(void);
static void CloseMatViewIncrementalMaintenance(void);

static char *get_null_condition_string(IvmOp op, char *arg1, char *arg2, char* count_col);
static char *get_operation_string(IvmOp op, char *col, char *arg1, char *arg2,
								  char* count_col, const char *castType);
static Query* rewrite_query_for_preupdate_state(Query *query, List *tables, TransactionId xid, CommandId cid, ParseState *pstate);
static Query *rewrite_query_for_counting_and_aggregation(Query *query, ParseState *pstate);
static void calc_delta(MV_TriggerTable *table, int index, Query *query,
						DestReceiver *dest_old, DestReceiver *dest_new, QueryEnvironment *queryEnv);
static RangeTblEntry* union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix, QueryEnvironment *queryEnv);
static char *make_delta_enr_name(const char *prefix, Oid relid, int count);
static void register_delta_ENRs(ParseState *pstate, Query *query, List *tables);
static void apply_delta(Oid matviewOid, Oid tempOid_new, Oid tempOid_old, Query *query, char *count_colname);
static void truncate_view_delta(Oid delta_oid);
static void clean_up_immediate_maintenance(MV_TriggerHashEntry *entry, Oid tempOid_old, Oid tempOid_new);

static void mv_InitHashTables(void);
static SPIPlanPtr mv_FetchPreparedPlan(MV_QueryKey *key);
static void mv_HashPreparedPlan(MV_QueryKey *key, SPIPlanPtr plan);
static void mv_BuildQueryKey(MV_QueryKey *key, Oid matview_id, int32 query_type);
static SPIPlanPtr get_plan_for_recalc_min_max(Oid matviewOid, const char *min_max_list,
						  const char *group_keys, int nkeys, Oid *keyTypes, bool with_group);
static SPIPlanPtr get_plan_for_set_min_max(Oid matviewOid, char *matviewname, const char *min_max_list,
						  int num_min_max, Oid *valTypes, bool with_group);

static Query *get_matview_query(Relation matviewRel);


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
 * the relfilenodes of the new table and the old materialized view, so the OID
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
				   ParamListInfo params, char *completionTag)
{
	Oid			matviewOid;
	Relation	matviewRel;
	Query	   *dataQuery;
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
				 errmsg("CONCURRENTLY and WITH NO DATA options cannot be used together")));


	dataQuery = get_matview_query(matviewRel);

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

	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also arrange to make GUC variable changes local to this command.
	 * Don't lock it down too tight to create a temporary table just yet.  We
	 * will switch modes when we are about to execute user code.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	save_nestlevel = NewGUCNestLevel();

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

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	OIDNewHeap = make_new_heap(matviewOid, tableSpace, relpersistence,
							   ExclusiveLock);
	LockRelationOid(OIDNewHeap, AccessExclusiveLock);
	dest = CreateTransientRelDestReceiver(OIDNewHeap);

	/*
	 * Now lock down security-restricted operations.
	 */
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	/* Generate the data, if wanted. */
	if (!stmt->skipData)
		processed = refresh_matview_datafill(dest, dataQuery, NULL, queryString);

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
		 * Inform stats collector about our activity: basically, we truncated
		 * the matview and inserted some new data.  (The concurrent code path
		 * above doesn't need to worry about this because the inserts and
		 * deletes it issues get counted by lower-level code.)
		 */
		pgstat_count_truncate(matviewRel);
		if (!stmt->skipData)
			pgstat_count_heap_insert(matviewRel, processed);
	}

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	ObjectAddressSet(address, RelationRelationId, matviewOid);

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
	plan = pg_plan_query(query, 0, NULL);

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

	/*
	 * We can skip WAL-logging the insertions, unless PITR or streaming
	 * replication is in use. We can skip the FSM in any case.
	 */
	myState->ti_options = TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN;
	if (!XLogIsNeeded())
		myState->ti_options |= TABLE_INSERT_SKIP_WAL;
	myState->bistate = GetBulkInsertState();

	/* Not using WAL requires smgr_targblock be initially invalid */
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
 *
 * This leaks memory through palloc(), which won't be cleaned up until the
 * current memory context is freed.
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
	 */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "SELECT newdata FROM %s newdata "
					 "WHERE newdata IS NOT NULL AND EXISTS "
					 "(SELECT 1 FROM %s newdata2 WHERE newdata2 IS NOT NULL "
					 "AND newdata2 OPERATOR(pg_catalog.*=) newdata "
					 "AND newdata2.ctid OPERATOR(pg_catalog.<>) "
					 "newdata.ctid)",
					 tempname, tempname);
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
					 "SELECT mv.ctid AS tid, newdata "
					 "FROM %s mv FULL JOIN %s newdata ON (",
					 diffname, matviewname, tempname);

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
						   " AND newdata OPERATOR(pg_catalog.*=) mv) "
						   "WHERE newdata IS NULL OR mv IS NULL "
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
 * IVM trigger function
 */

Datum
IVM_immediate_before(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	char		*matviewname = trigdata->tg_trigger->tgargs[0];
	List	*names = stringToQualifiedNameList(matviewname);
	Oid	matviewOid;

	MV_TriggerHashEntry *entry;
	bool	found;

	/*
	 * Wait for concurrent transactions which update this materialized view at READ COMMITED.
	 * This is needed to see changes commited in othre transactions. No wait and raise an error
	 * at REPEATABLE READ or SERIALIZABLE to prevent anormal update of matviews.
	 * XXX: dead-lock is possible here.
	 */
	if (!IsolationUsesXactSnapshot())
		matviewOid = RangeVarGetRelid(makeRangeVarFromNameList(names), ExclusiveLock, true);
	else
		matviewOid = RangeVarGetRelidExtended(makeRangeVarFromNameList(names), ExclusiveLock, RVR_MISSING_OK | RVR_NOWAIT, NULL, NULL);

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_query_cache)
		mv_InitHashTables();

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_ENTER, &found);

	/* On the first BEFORE to update the view, initialize trigger data */
	if (!found)
	{
		Snapshot snapshot = GetActiveSnapshot();

		entry->matview_id = matviewOid;
		entry->before_trig_count = 0;
		entry->after_trig_count = 0;
		entry->xid = GetCurrentTransactionId();
		entry->cid = snapshot->curcid;
		entry->tables = NIL;
		entry->has_old = false;
		entry->has_new = false;

	}

	entry->before_trig_count++;

	return PointerGetDatum(NULL);
}

Datum
IVM_immediate_maintenance(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel;
	Oid relid;
	Oid matviewOid;
	Query	   *query, *rewritten, *rewritten2;
	char*		matviewname = trigdata->tg_trigger->tgargs[0];
	char*		count_colname = trigdata->tg_trigger->tgargs[1];
	List	   *names;
	Relation matviewRel;
	int old_depth = matview_maintenance_depth;

	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDDelta_new = InvalidOid;
	Oid			OIDDelta_old = InvalidOid;
	DestReceiver *dest_new = NULL, *dest_old = NULL;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	MV_TriggerHashEntry *entry;
	MV_TriggerTable	*table;
	bool	found;
	ListCell   *lc;
	MemoryContext oldcxt;


	QueryEnvironment *queryEnv = create_queryEnv();
	ParseState *pstate;

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	rel = trigdata->tg_relation;
	relid = rel->rd_id;

	names = stringToQualifiedNameList(matviewname);
	matviewOid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true);


	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_query_cache)
		mv_InitHashTables();

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);

	Assert (entry != NULL);

	entry->after_trig_count++;

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
		oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		table = (MV_TriggerTable *) palloc0(sizeof(MV_TriggerTable));
		table->table_id = relid;
		table->old_tuplestores = NIL;
		table->new_tuplestores = NIL;
		table->old_rtes = NIL;
		table->new_rtes = NIL;
		table->rte_indexes = NIL;
		entry->tables = lappend(entry->tables, table);

		MemoryContextSwitchTo(oldcxt);
	}

	if (trigdata->tg_oldtable)
	{
		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
		table->old_tuplestores = lappend(table->old_tuplestores, trigdata->tg_oldtable);
		entry->has_old = true;
		MemoryContextSwitchTo(oldcxt);
	}
	if (trigdata->tg_newtable)
	{
		oldcxt = MemoryContextSwitchTo(CurTransactionContext);
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

		SetTransitionTablePreserved(relid, cmd);
	}


	/* If this is not the last AFTER trigger call, immediately exit. */
	Assert (entry->before_trig_count >= entry->after_trig_count);
	if (entry->before_trig_count != entry->after_trig_count)
		return PointerGetDatum(NULL);


	/* If this is the last AFTER trigger call, update the view. */

	matviewRel = table_open(matviewOid, NoLock);

	/* get view query*/
	query = get_matview_query(matviewRel);

	/* Make sure it is a materialized view. */
	Assert(matviewRel->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Get and push the latast snapshot to see any changes which is commited during waiting in
	 * other transactions at READ COMMITTED level.
	 * XXX: Is this safe?
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	 // XXX: necesarry?
	CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

	/* rewrite query */
	rewritten = copyObject(query);
	if (rewritten->hasSubLinks)
		convert_EXISTS_sublink_to_lateral_join(rewritten);
	rewritten = rewrite_query_for_preupdate_state(rewritten, entry->tables, entry->xid, entry->cid, pstate);
	rewritten = rewrite_query_for_counting_and_aggregation(rewritten, pstate);
	rewritten2 = copyObject(query);
	if (rewritten2->hasSubLinks)
		convert_EXISTS_sublink_to_lateral_join(rewritten2);

	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also arrange to make GUC variable changes local to this command.
	 * Don't lock it down too tight to create a temporary table just yet.  We
	 * will switch modes when we are about to execute user code.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	save_nestlevel = NewGUCNestLevel();

	/* Create temporary tables to store view deltas */
	tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
	if (entry->has_old)
	{
		OIDDelta_old = make_new_heap(matviewOid, tableSpace, RELPERSISTENCE_TEMP,
									 ExclusiveLock);
		LockRelationOid(OIDDelta_old, AccessExclusiveLock);
		dest_old = CreateTransientRelDestReceiver(OIDDelta_old);
	}
	if (entry->has_new)
	{
		if (entry->has_old)
			OIDDelta_new = make_new_heap(OIDDelta_old, tableSpace, RELPERSISTENCE_TEMP,
										 ExclusiveLock);
		else
			OIDDelta_new = make_new_heap(matviewOid, tableSpace, RELPERSISTENCE_TEMP,
										 ExclusiveLock);
		LockRelationOid(OIDDelta_new, AccessExclusiveLock);
		dest_new = CreateTransientRelDestReceiver(OIDDelta_new);
	}

	/*
	 * Now lock down security-restricted operations.
	 */
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	/* for all modified tables */
	foreach(lc, entry->tables)
	{
		ListCell *lc2;

		table = (MV_TriggerTable *) lfirst(lc);

		/* loop for self-join */
		foreach(lc2, table->rte_indexes)
		{
			int index = lfirst_int(lc2);
			calc_delta(table, index, rewritten, dest_old, dest_new, queryEnv);

			PG_TRY();
			{
				apply_delta(matviewOid, OIDDelta_new, OIDDelta_old, rewritten2,count_colname);
			}
			PG_CATCH();
			{
				matview_maintenance_depth = old_depth;
				PG_RE_THROW();
			}
			PG_END_TRY();

			/* truncate view delta tables */
			truncate_view_delta(OIDDelta_old);
			truncate_view_delta(OIDDelta_new);
		}
	}

	/* Pop the original snapshot. */
	PopActiveSnapshot();

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Clean up hash entry and drop temporary tables */
	clean_up_immediate_maintenance(entry, OIDDelta_old, OIDDelta_new);

	return PointerGetDatum(NULL);
}

#define IVM_colname(type, col) makeObjectName("__ivm_" type, col, "_")

static char *
get_null_condition_string(IvmOp op, char *arg1, char *arg2, char* count_col)
{
	StringInfoData null_cond;
	initStringInfo(&null_cond);

	switch (op)
	{
		case IVM_ADD:
			appendStringInfo(&null_cond,
				"%s = 0 AND %s = 0",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		case IVM_SUB:
			appendStringInfo(&null_cond,
				"%s = %s",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		default:
			elog(ERROR,"unkwon opration");
	}

	return null_cond.data;
}

static char *
get_operation_string(IvmOp op, char *col, char *arg1, char *arg2, char* count_col, const char *castType)
{
	StringInfoData buf, castString;
	char	*col1 = quote_qualified_identifier(arg1, col);
	char	*col2 = quote_qualified_identifier(arg2, col);

	char op_char = (op == IVM_SUB ? '-' : '+');
	char *sign = (op == IVM_SUB ? "-" : "");

	initStringInfo(&buf);
	initStringInfo(&castString);

	if (castType)
		appendStringInfo(&castString, "::%s", castType);


	if (!count_col)
	{
		appendStringInfo(&buf, "(%s %c %s)%s",
			col1, op_char, col2, castString.data);
	}
	else
	{
		char *null_cond = get_null_condition_string(op, arg1, arg2, count_col);

		appendStringInfo(&buf,
			"(CASE WHEN %s THEN NULL "
				" WHEN %s IS NULL THEN %s%s "
				" WHEN %s IS NULL THEN (%s) "
				" ELSE (%s %c %s)%s END)",
			null_cond,
			col1, sign, col2,
			col2, col1,
			col1, op_char, col2, castString.data
		);
	}

	return buf.data;
}


static void
calc_delta(MV_TriggerTable *table, int index, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new, QueryEnvironment *queryEnv)
{
	ListCell *lc;
	ListCell *lc2;
	RangeTblEntry *rte;

/*
	lc = list_nth_cell(query->rtable, index);
	rte = (RangeTblEntry *) lfirst(lc);

	if (rte->subquery)
	{
		ListCell *lc2;
		RangeTblEntry *rte2;

		foreach(lc2, rte->subquery->rtable)
		{
			rte2 = (RangeTblEntry *) lfirst(lc2);
		}
	}
*/

	foreach(lc, query->rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);
		/*
		 * Currently, it is not supported that a subquery is contained other subquery.
		 */
		if (rte->subquery && rte->relid != table->table_id)
		{
			lc2 = list_nth_cell(rte->subquery->rtable, index);
			rte = (RangeTblEntry *) lfirst(lc2);
			if (rte == NULL || rte->relid != table->table_id )
				continue;

			/* found tartget rte */
			lc = lc2;
			break;
		}
		else if (rte != NULL && rte->relid == table->table_id)
		{
			break;
		}
	}

	/* Generate old delta */
	if (list_length(table->old_rtes) > 0)
	{
		lfirst(lc) = union_ENRs(rte, table->table_id, table->old_rtes, "old", queryEnv);
		refresh_matview_datafill(dest_old, query, queryEnv, NULL);
	}
	/* Generate new delta */
	if (list_length(table->new_rtes) > 0)
	{
		lfirst(lc) = union_ENRs(rte, table->table_id, table->new_rtes, "new", queryEnv);
		refresh_matview_datafill(dest_new, query, queryEnv, NULL);
	}

	/* Retore the original RTE */
	lfirst(lc) = table->original_rte;
}


static RangeTblEntry*
union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix, QueryEnvironment *queryEnv)
{
	StringInfoData str;
	ParseState	*pstate;
	RawStmt *raw;
	Query *sub;
	int	i;

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	initStringInfo(&str);

	for (i = 0; i < list_length(enr_rtes); i++)
	{
		if (i > 0)
			appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str," SELECT * FROM %s", make_delta_enr_name(prefix, relid, i));
	}

	raw = (RawStmt*)linitial(raw_parser(str.data));
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

	return rte;
}

static char*
make_delta_enr_name(const char *prefix, Oid relid, int count)
{
	char buf[NAMEDATALEN];
	char *name;

	snprintf(buf, NAMEDATALEN, "%s_%u_%u", prefix, relid, count);
	name = pstrdup(buf);

	return name;
}

static void
register_delta_ENRs(ParseState *pstate, Query *query, List *tables)
{
	QueryEnvironment *queryEnv = pstate->p_queryEnv;
	ListCell *lc;
	RangeTblEntry	*rte;
	bool flag = false;

	foreach(lc, tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);
		ListCell *lc2;
		int count;

		foreach(lc2, query->rtable)
		{
			RangeTblEntry *r = (RangeTblEntry*) lfirst(lc2);

			if (r->relid == table->table_id)
			{
				flag = true;
				break;
			}
		}

		if (!flag)
			continue;

		count = 0;
		foreach(lc2, table->old_tuplestores)
		{
			Tuplestorestate *oldtable = (Tuplestorestate *) lfirst(lc2);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));

			enr->md.name = make_delta_enr_name("old", table->table_id, count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = NULL;
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(oldtable);
			enr->reldata = oldtable;
			register_ENR(queryEnv, enr);

			rte = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			/* subquery is not support */
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

			enr->md.name = make_delta_enr_name("new", table->table_id, count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = NULL;
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(newtable);
			enr->reldata = newtable;
			register_ENR(queryEnv, enr);

			rte = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			query->rtable = lappend(query->rtable, rte);
			table->new_rtes = lappend(table->new_rtes, rte);

			count++;
		}
	}
}

static RangeTblEntry*
get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table, TransactionId xid, CommandId cid, QueryEnvironment *queryEnv)
{
	StringInfoData str;
	RawStmt *raw;
	Query *sub;
	Relation rel;
	ParseState *pstate;
	char *relname;
	int i;

	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * We can use NoLock here since AcquireRewriteLocks should
	 * have locked the rel already.
	 */
	rel = table_open(table->table_id, NoLock);
	relname = quote_qualified_identifier(
					get_namespace_name(RelationGetNamespace(rel)),
									   RelationGetRelationName(rel));
	table_close(rel, NoLock);

	initStringInfo(&str);
	appendStringInfo(&str,
		"SELECT t.* FROM %s t"
		" WHERE (age(t.xmin) - age(%u::text::xid) > 0) OR"
		" (t.xmin = %u AND t.cmin::text::int < %u)",
			relname, xid, xid, cid);

	for (i=0; i<list_length(table->old_tuplestores); i++)
	{
		appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str," SELECT * FROM %s", make_delta_enr_name("old", table->table_id, i));
	}

	raw = (RawStmt*)linitial(raw_parser(str.data));
	sub = transformStmt(pstate, raw->stmt);

	table->original_rte = copyObject(rte);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = sub;
	rte->security_barrier = false;
	/* Clear fields that should not be set in a subquery RTE */
//	rte->relid = InvalidOid;
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

static Query*
rewrite_query_for_preupdate_state(Query *query, List *tables, TransactionId xid, CommandId cid, ParseState *pstate)
{
	ListCell *lc;
	int num_rte = list_length(query->rtable);
	int i;

	register_delta_ENRs(pstate, query, tables);

	// XXX: Is necessary? Is this right timing?
	AcquireRewriteLocks(query, true, false);

	foreach(lc, tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);
		ListCell *lc2;

		i = 0;
		foreach(lc2, query->rtable)
		{
			RangeTblEntry *r = (RangeTblEntry*) lfirst(lc2);

			if (r->rtekind == RTE_SUBQUERY)
			{
				rewrite_query_for_preupdate_state(r->subquery, tables, xid, cid, pstate);
			}
			else if (r->relid == table->table_id)
			{
				lfirst(lc2) = get_prestate_rte(r, table, xid, cid, pstate->p_queryEnv);
				table->rte_indexes = lappend_int(table->rte_indexes, i);
			}

			if (++i >= num_rte)
				break;
		}
	}

	return query;
}

static Query *
rewrite_query_for_counting_and_aggregation(Query *query, ParseState *pstate)
{
	TargetEntry *tle_count;
	FuncCall *fn;
	Node *node;
	Const	*dmy_arg = makeConst(INT4OID,
								 -1,
								 InvalidOid,
								 sizeof(int32),
								 Int32GetDatum(1),
								 false,
								 true);
	ListCell *tbl_lc;

	if (query->hasAggs)
	{
		ListCell *lc;
		List *agg_counts = NIL;
		AttrNumber next_resno = list_length(query->targetList) + 1;

		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (IsA(tle->expr, Aggref))
			{
				Aggref *aggref = (Aggref *) tle->expr;
				const char *aggname = get_func_name(aggref->aggfnoid);

				/*
				 * For aggregate functions except to count, add count func with the same arg parameters.
				 * Also, add sum func for agv.
				 *
				 * XXX: need some generalization
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
											NULL,
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
												NULL,
												false);
					agg_counts = lappend(agg_counts, tle_count);
					next_resno++;
					}
				}

			}
			query->targetList = list_concat(query->targetList, agg_counts);
	}

	/* Add count(*) using EXISTS clause */
	foreach(tbl_lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(tbl_lc);
		if (rte->subquery)
		{
			/* search subquery in RangeTblEntry */
			pstate->p_rtable = query->rtable;
			node = scanRTEForColumn(pstate,rte,"__ivm_exists_count__",-1, 0, NULL);
			if(node == NULL)
				continue;
			tle_count = makeTargetEntry((Expr *) node,
										list_length(query->targetList) + 1,
										pstrdup("__ivm_exists_count__"),
										false);
			query->targetList = lappend(query->targetList, tle_count);
			break;
		}
	}

	fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
	fn->agg_star = true;
	if (!query->groupClause && !query->hasAggs)
		query->groupClause = transformDistinctClause(NULL, &query->targetList, query->sortClause, false);

	node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

	tle_count = makeTargetEntry((Expr *) node,
							  list_length(query->targetList) + 1,
							  NULL,
							  false);
	query->targetList = lappend(query->targetList, tle_count);

	query->hasAggs = true;

	return query;
}


static void
apply_delta(Oid matviewOid, Oid tempOid_new, Oid tempOid_old, Query *query, char * count_colname)
{
	StringInfoData querybuf;
	StringInfoData mvatts_buf, diffatts_buf;
	StringInfoData mv_gkeys_buf, diff_gkeys_buf, updt_gkeys_buf;
	StringInfoData diff_aggs_buf, update_aggs_old, update_aggs_new;
	StringInfoData returning_buf, result_buf;
	StringInfoData min_or_max_buf;
	Relation	matviewRel;
	Relation	tempRel_new = NULL, tempRel_old = NULL;
	char	   *matviewname;
	char	   *tempname_new = NULL, *tempname_old = NULL;
	ListCell	*lc;
	char	   *sep, *sep_agg;
	bool		with_group = query->groupClause != NULL;
	int			i;
	bool		has_min_or_max = false;
	int			num_group_keys = 0;
	int			num_min_or_max = 0;


	initStringInfo(&querybuf);
	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));
	if (OidIsValid(tempOid_new))
	{
		tempRel_new = table_open(tempOid_new, NoLock);
		tempname_new = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel_new)),
												  RelationGetRelationName(tempRel_new));
	}
	if (OidIsValid(tempOid_old))
	{
		tempRel_old = table_open(tempOid_old, NoLock);
		tempname_old = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel_old)),
												  RelationGetRelationName(tempRel_old));
	}

	initStringInfo(&mvatts_buf);
	initStringInfo(&diffatts_buf);
	initStringInfo(&diff_aggs_buf);
	initStringInfo(&update_aggs_old);
	initStringInfo(&update_aggs_new);
	initStringInfo(&returning_buf);
	initStringInfo(&result_buf);
	initStringInfo(&min_or_max_buf);

	sep = "";
	sep_agg= "";
	i = 0;
	foreach (lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		i++;

		if (tle->resjunk)
			continue;

		appendStringInfo(&mvatts_buf, "%s", sep);
		appendStringInfo(&diffatts_buf, "%s", sep);
		sep = ", ";

		appendStringInfo(&mvatts_buf, "%s", quote_qualified_identifier("mv", resname));
		appendStringInfo(&diffatts_buf, "%s", quote_qualified_identifier("diff", resname));

		if (query->hasAggs && IsA(tle->expr, Aggref))
		{
			Aggref *aggref = (Aggref *) tle->expr;
			const char *aggname = get_func_name(aggref->aggfnoid);
			const char *aggtype = format_type_be(aggref->aggtype); /* XXX: should be add_cast_to ? */

			appendStringInfo(&update_aggs_old, "%s", sep_agg);
			appendStringInfo(&update_aggs_new, "%s", sep_agg);
			appendStringInfo(&diff_aggs_buf, "%s", sep_agg);

			sep_agg = ", ";

			/* XXX: need some generalization
			 *
			 * Specifically, Using func names is not robust.  We can use oids instead
			 * of names, but it would be nice to add some information to pg_aggregate
			 * and handler functions.
			 */

			if (!strcmp(aggname, "count"))
			{
				/* resname = mv.resname - t.resname */
				appendStringInfo(&update_aggs_old,
					"%s = %s",
					quote_qualified_identifier(NULL, resname),
					get_operation_string(IVM_SUB,resname, "mv", "t", NULL, NULL));

				/* resname = mv.resname + diff.resname */
				appendStringInfo(&update_aggs_new,
					"%s = %s",
					quote_qualified_identifier(NULL, resname),
					get_operation_string(IVM_ADD, resname, "mv", "diff", NULL, NULL));

				appendStringInfo(&diff_aggs_buf, "%s",
					quote_qualified_identifier("diff", resname)
				);
			}
			else if (!strcmp(aggname, "sum"))
			{
				char *count_col = IVM_colname("count", resname);

				/* sum = mv.sum - t.sum */
				appendStringInfo(&update_aggs_old,
					"%s = %s, ",
					quote_qualified_identifier(NULL, resname),
					get_operation_string(IVM_SUB, resname, "mv", "t", count_col, NULL)
				);
				/* count = mv.count - t.count */
				appendStringInfo(&update_aggs_old,
					"%s = %s",
					quote_qualified_identifier(NULL, count_col),
					get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
				);

				/* sum = mv.sum + diff.sum */
				appendStringInfo(&update_aggs_new,
					"%s = %s, ",
					quote_qualified_identifier(NULL, resname),
					get_operation_string(IVM_ADD, resname, "mv", "diff", count_col, NULL)
				);
				/* count = mv.count + diff.count */
				appendStringInfo(&update_aggs_new,
					"%s = %s",
					quote_qualified_identifier(NULL, count_col),
					get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
				);

				appendStringInfo(&diff_aggs_buf, "%s, %s",
					quote_qualified_identifier("diff", resname),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
				);
			}
			else if (!strcmp(aggname, "avg"))
			{
				char *sum_col = IVM_colname("sum", resname);
				char *count_col = IVM_colname("count", resname);

				/* avg = (mv.sum - t.sum)::aggtype / (mv.count - t.count) */
				appendStringInfo(&update_aggs_old,
					"%s = %s / %s, ",
					quote_qualified_identifier(NULL, resname),
					get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, aggtype),
					get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
				);
				/* sum = mv.sum - t.sum */
				appendStringInfo(&update_aggs_old,
					"%s = %s, ",
					quote_qualified_identifier(NULL, sum_col),
					get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, NULL)
				);
				/* count = mv.count - t.count */
				appendStringInfo(&update_aggs_old,
					"%s = %s",
					quote_qualified_identifier(NULL, count_col),
					get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
				);

				/* avg = (mv.sum + diff.sum)::aggtype / (mv.count + diff.count) */
				appendStringInfo(&update_aggs_new,
					"%s = %s / %s, ",
					quote_qualified_identifier(NULL, resname),
					get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, aggtype),
					get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
				);
				/* sum = mv.sum + diff.sum */
				appendStringInfo(&update_aggs_new,
					"%s = %s, ",
					quote_qualified_identifier(NULL, sum_col),
					get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, NULL)
				);
				/* count = mv.count + diff.count */
				appendStringInfo(&update_aggs_new,
					"%s = %s",
					quote_qualified_identifier(NULL, count_col),
					get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
				);

				appendStringInfo(&diff_aggs_buf, "%s, %s, %s",
					quote_qualified_identifier("diff", resname),
					quote_qualified_identifier("diff", makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
				);
			}
			else if (!strcmp(aggname, "min") || !strcmp(aggname, "max"))
			{
				bool is_min = !strcmp(aggname, "min");
				char *count_col = IVM_colname("count", resname);

				if (!has_min_or_max)
				{
					has_min_or_max = true;
					appendStringInfo(&returning_buf, "RETURNING mv.ctid, (");
				}
				else
				{
					appendStringInfo(&returning_buf, " OR ");
					appendStringInfo(&min_or_max_buf, ",");
				}

				appendStringInfo(&returning_buf, "%s %s %s",
					quote_qualified_identifier("mv", resname),
					is_min ? ">=" : "<=",
					quote_qualified_identifier("t", resname)
				);
				appendStringInfo(&min_or_max_buf, "%s", quote_qualified_identifier(NULL, resname));

				/* Even if the new values is not NULL, this might be recomputated afterwords. */
				appendStringInfo(&update_aggs_old,
					"%s = CASE WHEN %s THEN NULL ELSE %s END, ",
					quote_qualified_identifier(NULL, resname),
					get_null_condition_string(IVM_SUB, "mv", "t", count_col),
					quote_qualified_identifier("mv", resname)
				);
				/* count = mv.count - t.count */
				appendStringInfo(&update_aggs_old,
					"%s = %s",
					quote_qualified_identifier(NULL, count_col),
					get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
				);

				/*
				 * min = least(mv.min, diff.min)
				 * max = greatest(mv.max, diff.max)
				 */
				appendStringInfo(&update_aggs_new,
					"%s = CASE WHEN %s THEN NULL ELSE %s(%s,%s) END, ",
					quote_qualified_identifier(NULL, resname),
					get_null_condition_string(IVM_ADD, "mv", "diff", count_col),

					is_min ? "least" : "greatest",
					quote_qualified_identifier("mv", resname),
					quote_qualified_identifier("diff", resname)
				);

				/* count = mv.count + diff.count */
				appendStringInfo(&update_aggs_new,
					"%s = %s",
					quote_qualified_identifier(NULL, count_col),
					get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
				);

				appendStringInfo(&diff_aggs_buf, "%s, %s",
					quote_qualified_identifier("diff", resname),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
				);

				num_min_or_max++;
			}
			else
				elog(ERROR, "unsupported aggregate function: %s", aggname);

		}
	}
	if (has_min_or_max)
		appendStringInfo(&returning_buf, ") AS recalc");

	if (query->hasAggs)
	{
		initStringInfo(&mv_gkeys_buf);
		initStringInfo(&diff_gkeys_buf);
		initStringInfo(&updt_gkeys_buf);

		if (with_group)
		{
			sep_agg= "";
			foreach (lc, query->groupClause)
			{
				SortGroupClause *sgcl = (SortGroupClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sgcl, query->targetList);

				Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno-1);
				char *resname = NameStr(attr->attname);

				appendStringInfo(&mv_gkeys_buf, "%s", sep_agg);
				appendStringInfo(&diff_gkeys_buf, "%s", sep_agg);
				appendStringInfo(&updt_gkeys_buf, "%s", sep_agg);

				sep_agg = ", ";

				appendStringInfo(&mv_gkeys_buf, "%s", quote_qualified_identifier("mv", resname));
				appendStringInfo(&diff_gkeys_buf, "%s", quote_qualified_identifier("diff", resname));
				appendStringInfo(&updt_gkeys_buf, "%s", quote_qualified_identifier("updt", resname));

				num_group_keys++;
			}

			if (has_min_or_max)
			{
				appendStringInfo(&returning_buf, ", %s", mv_gkeys_buf.data);
				appendStringInfo(&result_buf, "SELECT ctid AS tid, %s FROM updt WHERE recalc", updt_gkeys_buf.data);
			}
		}
		else
		{
			appendStringInfo(&mv_gkeys_buf, "1");
			appendStringInfo(&diff_gkeys_buf, "1");
			appendStringInfo(&updt_gkeys_buf, "1");
		}
	}


	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Analyze the temp table with the new contents. */
	if (tempname_new)
	{
		appendStringInfo(&querybuf, "ANALYZE %s", tempname_new);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}
	if (tempname_old)
	{
		resetStringInfo(&querybuf);
		appendStringInfo(&querybuf, "ANALYZE %s", tempname_old);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	OpenMatViewIncrementalMaintenance();

	if (query->hasAggs)
	{
		if (tempname_old)
		{
			resetStringInfo(&querybuf);
			appendStringInfo(&querybuf,
							"WITH t AS ("
							"  SELECT diff.%s, "
							"         %s,"
							"         (diff.%s = mv.%s) AS for_dlt, "
							"         mv.ctid"
							"  FROM %s AS mv, %s AS diff WHERE (%s) = (%s)"
							"), updt AS ("
							"  UPDATE %s AS mv SET %s = mv.%s - t.%s,"
							"                      %s"
							"  FROM t WHERE mv.ctid = t.ctid AND NOT for_dlt"
							"   %s"
							"), dlt AS ("
							"  DELETE FROM %s AS mv USING t WHERE mv.ctid = t.ctid AND for_dlt "
							") %s",
							count_colname,
							diff_aggs_buf.data,
							count_colname, count_colname,
							matviewname, tempname_old, mv_gkeys_buf.data, diff_gkeys_buf.data,
							matviewname, count_colname, count_colname, count_colname, update_aggs_old.data,
							returning_buf.data,
							matviewname,
							(has_min_or_max && with_group) ? result_buf.data : "SELECT 1"
							);
			if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
				elog(ERROR, "SPI_exec failed: %s", querybuf.data);

			if (has_min_or_max && SPI_processed > 0)
			{
				SPITupleTable *tuptable_recalc = SPI_tuptable;
				TupleDesc   tupdesc_recalc = tuptable_recalc->tupdesc;
				int64		processed = SPI_processed;
				uint64      i;
				Oid			*keyTypes = NULL, *minmaxTypes = NULL;
				char		*keyNulls = NULL, *minmaxNulls = NULL;
				Datum		*keyVals = NULL, *minmaxVals = NULL;

				if (with_group)
				{
					keyTypes = palloc(sizeof(Oid) * num_group_keys);
					keyNulls = palloc(sizeof(char) * num_group_keys);
					keyVals = palloc(sizeof(Datum) * num_group_keys);
					Assert(tupdesc_recalc->natts == num_group_keys + 1);

					for (i = 0; i < num_group_keys; i++)
						keyTypes[i] = TupleDescAttr(tupdesc_recalc, i+1)->atttypid;
				}

				minmaxTypes = palloc(sizeof(Oid) * (num_min_or_max + 1));
				minmaxNulls = palloc(sizeof(char) * (num_min_or_max + 1));
				minmaxVals = palloc(sizeof(Datum) * (num_min_or_max + 1));

				for (i=0; i< processed; i++)
				{
					int j;
					bool isnull;
					SPIPlanPtr plan;
					SPITupleTable *tuptable_minmax;
					TupleDesc   tupdesc_minmax;

					if (with_group)
					{
						for (j = 0; j < num_group_keys; j++)
						{
							keyVals[j] = SPI_getbinval(tuptable_recalc->vals[i], tupdesc_recalc, j+2, &isnull);
							if (isnull)
								keyNulls[j] = 'n';
							else
								keyNulls[j] = ' ';
						}
					}

					plan = get_plan_for_recalc_min_max(matviewOid, min_or_max_buf.data,
													   mv_gkeys_buf.data, num_group_keys, keyTypes, with_group);

					if (SPI_execute_plan(plan, keyVals, keyNulls, false, 0) != SPI_OK_SELECT)
						elog(ERROR, "SPI_execcute_plan1");
					if (SPI_processed != 1)
						elog(ERROR, "SPI_execcute_plan returned zere or more than one rows");

					tuptable_minmax = SPI_tuptable;
					tupdesc_minmax = tuptable_minmax->tupdesc;

					Assert(tupdesc_minmax->natts == num_min_or_max);

					for (j = 0; j < tupdesc_minmax->natts; j++)
					{
						if (i == 0)
							minmaxTypes[j] = TupleDescAttr(tupdesc_minmax, j)->atttypid;

						minmaxVals[j] = SPI_getbinval(tuptable_minmax->vals[0], tupdesc_minmax, j+1, &isnull);
						if (isnull)
							minmaxNulls[j] = 'n';
						else
							minmaxNulls[j] = ' ';
					}
					minmaxTypes[j] = TIDOID;
					minmaxVals[j] = SPI_getbinval(tuptable_recalc->vals[i], tupdesc_recalc, 1, &isnull);
					minmaxNulls[j] = ' ';

					plan = get_plan_for_set_min_max(matviewOid, matviewname, min_or_max_buf.data,
													num_min_or_max, minmaxTypes, with_group);

					if (SPI_execute_plan(plan, minmaxVals, minmaxNulls, false, 0) != SPI_OK_UPDATE)
						elog(ERROR, "SPI_execcute_plan2");

				}
			}

		}
		if (tempname_new)
		{
			resetStringInfo(&querybuf);
			appendStringInfo(&querybuf,
							"WITH updt AS ("
							"  UPDATE %s AS mv SET %s = mv.%s + diff.%s"
							", %s "
							"  FROM %s AS diff WHERE (%s) = (%s)"
							"  RETURNING %s"
							") INSERT INTO %s (SELECT * FROM %s AS diff WHERE (%s) NOT IN (SELECT %s FROM updt));",
							matviewname, count_colname, count_colname, count_colname, update_aggs_new.data, tempname_new,
							mv_gkeys_buf.data, diff_gkeys_buf.data, diff_gkeys_buf.data,
							matviewname, tempname_new, diff_gkeys_buf.data, updt_gkeys_buf.data);
			if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
				elog(ERROR, "SPI_exec failed: %s", querybuf.data);
		}
	}
	else
	{
		if (tempname_old)
		{
			resetStringInfo(&querybuf);
			appendStringInfo(&querybuf,
							"WITH t AS ("
							"  SELECT diff.%s, (diff.%s = mv.%s) AS for_dlt, mv.ctid"
							"  FROM %s AS mv, %s AS diff WHERE (%s) = (%s)"
							"), updt AS ("
							"  UPDATE %s AS mv SET %s = mv.%s - t.%s"
							"  FROM t WHERE mv.ctid = t.ctid AND NOT for_dlt"
							") DELETE FROM %s AS mv USING t WHERE mv.ctid = t.ctid AND for_dlt;",
							count_colname, count_colname, count_colname, matviewname, tempname_old, mvatts_buf.data, diffatts_buf.data,
							matviewname, count_colname,count_colname,count_colname, matviewname);
			if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
				elog(ERROR, "SPI_exec failed: %s", querybuf.data);
		}
		if (tempname_new)
		{
			resetStringInfo(&querybuf);
			appendStringInfo(&querybuf,
							"WITH updt AS ("
							"  UPDATE %s AS mv SET %s= mv.%s + diff.%s"
							"  FROM %s AS diff WHERE (%s) = (%s)"
							"  RETURNING %s"
							") INSERT INTO %s (SELECT * FROM %s AS diff WHERE (%s) NOT IN (SELECT * FROM updt));",
							matviewname, count_colname, count_colname, count_colname, tempname_new, mvatts_buf.data, diffatts_buf.data, diffatts_buf.data, matviewname, tempname_new, diffatts_buf.data);
			if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
				elog(ERROR, "SPI_exec failed: %s", querybuf.data);
		}
	}

	/* We're done maintaining the materialized view. */
	CloseMatViewIncrementalMaintenance();

	if (OidIsValid(tempOid_new))
		table_close(tempRel_new, NoLock);
	if (OidIsValid(tempOid_old))
		table_close(tempRel_old, NoLock);

	table_close(matviewRel, NoLock);


	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}


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
 * mv_HashPreparedPlan -
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

/* ----------
 * mv_BuildQueryKey -
 *
 *	Construct a hashtable key for a prepared SPI plan for IVM.
 *
 *		key: output argument, *key is filled in based on the other arguments
 *		matview_id: OID of materialized view
 *		query_type: an internal number identifying the query type
 *			(see MV_PLAN_XXX constants at head of file)
 * ----------
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

static SPIPlanPtr
get_plan_for_recalc_min_max(Oid matviewOid, const char *min_max_list,
							const char *group_keys, int nkeys, Oid *keyTypes, bool with_group)
{
	MV_QueryKey key;
	SPIPlanPtr	plan;

	/* Fetch or prepare a saved plan for the real check */
	mv_BuildQueryKey(&key, matviewOid, MV_PLAN_RECALC_MINMAX);

	if ((plan = mv_FetchPreparedPlan(&key)) == NULL)
	{
		char	*viewdef;
		StringInfoData	str;
		int		i;


		/* get view definition of matview */
		viewdef = text_to_cstring((text *) DatumGetPointer(
					DirectFunctionCall1(pg_get_viewdef, ObjectIdGetDatum(matviewOid))));
		/* get rid of tailling semi-collon */
		viewdef[strlen(viewdef)-1] = '\0';

		initStringInfo(&str);
		appendStringInfo(&str, "SELECT %s FROM (%s) mv", min_max_list, viewdef);

		if (with_group)
		{
			appendStringInfo(&str, " WHERE (%s) = (", group_keys);

			for (i = 1; i <= nkeys; i++)
				appendStringInfo(&str, "%s$%d", (i==1 ? "" : ", "),i );

			appendStringInfo(&str, ")");
		}

		plan = SPI_prepare(str.data, (with_group ? nkeys : 0), (with_group ? keyTypes : NULL));
		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), str.data);

		SPI_keepplan(plan);
		mv_HashPreparedPlan(&key, plan);
	}

	return plan;
}

static SPIPlanPtr
get_plan_for_set_min_max(Oid matviewOid, char *matviewname, const char *min_max_list,
						  int num_min_max, Oid *valTypes, bool with_group)
{
	MV_QueryKey key;
	SPIPlanPtr plan;

	/* Fetch or prepare a saved plan for the real check */
	mv_BuildQueryKey(&key, matviewOid, MV_PLAN_SET_MINMAX);

	if ((plan = mv_FetchPreparedPlan(&key)) == NULL)
	{
		StringInfoData str;
		int		i;

		initStringInfo(&str);
		appendStringInfo(&str, "UPDATE %s AS mv SET (%s) = (",
			matviewname, min_max_list);

		for (i = 1; i <= num_min_max; i++)
			appendStringInfo(&str, "%s$%d", (i==1 ? "" : ", "), i);

		appendStringInfo(&str, ")");

		if (with_group)
			appendStringInfo(&str, " WHERE ctid = $%d", num_min_max + 1);

		plan = SPI_prepare(str.data, num_min_max + (with_group ? 1 : 0), valTypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), str.data);

		SPI_keepplan(plan);
		mv_HashPreparedPlan(&key, plan);
	}

	return plan;
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

static void
truncate_view_delta(Oid delta_oid)
{
	Relation	rel;

	if (!OidIsValid(delta_oid))
		return;

	rel = table_open(delta_oid, NoLock);
	ExecuteTruncateGuts(list_make1(rel), list_make1_oid(delta_oid), NIL,
						DROP_RESTRICT, false);
	table_close(rel, NoLock);
}

static void
clean_up_immediate_maintenance(MV_TriggerHashEntry *entry, Oid tempOid_old, Oid tempOid_new)
{
	bool found;
	StringInfoData querybuf;
	Relation tempRel_old, tempRel_new;
	char *tempname_old = NULL, *tempname_new = NULL;
	ListCell *lc;

	foreach(lc, entry->tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);

		list_free(table->old_tuplestores);
		list_free(table->new_tuplestores);
	}
	list_free(entry->tables);

	hash_search(mv_trigger_info, (void *) &entry->matview_id, HASH_REMOVE, &found);

	if (OidIsValid(tempOid_new))
	{
		tempRel_new = table_open(tempOid_new, NoLock);
		tempname_new = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel_new)),
												  RelationGetRelationName(tempRel_new));
		table_close(tempRel_new, NoLock);
	}
	if (OidIsValid(tempOid_old))
	{
		tempRel_old = table_open(tempOid_old, NoLock);
		tempname_old = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel_old)),
												  RelationGetRelationName(tempRel_old));
		table_close(tempRel_old, NoLock);
	}

	initStringInfo(&querybuf);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Clean up temp tables. */
	if (OidIsValid(tempOid_old))
	{
		resetStringInfo(&querybuf);
		appendStringInfo(&querybuf, "DROP TABLE %s", tempname_old);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}
	if (OidIsValid(tempOid_new))
	{
		resetStringInfo(&querybuf);
		appendStringInfo(&querybuf, "DROP TABLE %s", tempname_new);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

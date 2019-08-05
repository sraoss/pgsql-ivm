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
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "nodes/print.h"
#include "catalog/pg_type_d.h"
#include "optimizer/optimizer.h"
#include "commands/defrem.h"


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

static int	matview_maintenance_depth = 0;

static SPIPlanPtr	plan_for_recalc_min_max = NULL;
static SPIPlanPtr	plan_for_set_min_max = NULL;
//static HTAB *query_cache_for_recalc_min_max = NULL;
//static HTAB *query_cache_for_set_min_max = NULL;

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

static void apply_delta(Oid matviewOid, Oid tempOid_new, Oid tempOid_old,
			Query *query, Oid relowner, int save_sec_context);
static SPIPlanPtr get_plan_for_recalc_min_max(Oid matviewOid, const char *min_max_list,
						  const char *group_keys, int nkeys, Oid *keyTypes, bool with_group);
static SPIPlanPtr get_plan_for_set_min_max(char *matviewname, const char *min_max_list,
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
IVM_immediate_maintenance(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel;
	Oid relid;
	Oid matviewOid;
	Query	   *query, *old_delta_qry, *new_delta_qry;
	char*		matviewname = trigdata->tg_trigger->tgargs[0];
	List	   *names;
	Relation matviewRel;
	int old_depth = matview_maintenance_depth;

	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDDelta_new = InvalidOid;
	Oid			OIDDelta_old = InvalidOid;
	DestReceiver *dest_new = NULL, *dest_old = NULL;
	char		relpersistence;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	ParseState *pstate;
	QueryEnvironment *queryEnv = create_queryEnv();

	Const	*dmy_arg = makeConst(INT4OID,
								 -1,
								 InvalidOid,
								 sizeof(int32),
								 Int32GetDatum(1),
								 false,
								 true); /* pass by value */

	/* Create a dummy ParseState for addRangeTableEntryForENR */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	names = stringToQualifiedNameList(matviewname);

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

	matviewRel = table_open(matviewOid, NoLock);

	/*
	 * Get and push the latast snapshot to see any changes which is commited during waiting in
	 * other transactions at READ COMMITTED level.
	 * XXX: Is this safe?
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Make sure it is a materialized view. */
	if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a materialized view",
						RelationGetRelationName(matviewRel))));

	rel = trigdata->tg_relation;
	relid = rel->rd_id;

	/* get view query*/
	query = get_matview_query(matviewRel);

	new_delta_qry = copyObject(query);
	old_delta_qry = copyObject(query);

	if (trigdata->tg_newtable)
	{
		RangeTblEntry *rte;
		ListCell   *lc;

		TargetEntry *tle;
		Node *node;
		FuncCall *fn;

		EphemeralNamedRelation enr =
			palloc(sizeof(EphemeralNamedRelationData));

		enr->md.name = trigdata->tg_trigger->tgnewtable;
		enr->md.reliddesc = trigdata->tg_relation->rd_id;
		enr->md.tupdesc = NULL;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(trigdata->tg_newtable);
		enr->reldata = trigdata->tg_newtable;
		register_ENR(queryEnv, enr);

		rte = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
		new_delta_qry->rtable = lappend(new_delta_qry->rtable, rte);

		foreach(lc, new_delta_qry->rtable)
		{
			RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);
			if (r->relid == relid)
			{
				lfirst(lc) = rte;
				break;
			}
		}

		if (query->hasAggs)
		{
			ListCell *lc;
			List *agg_counts = NIL;
			AttrNumber next_resno = list_length(query->targetList) + 1;
			Node *node;

			foreach(lc, query->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);
				TargetEntry *tle_count;

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
						fn = makeFuncCall(list_make1(makeString("sum")), NIL, -1);

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
				}

			}
			new_delta_qry->targetList = list_concat(new_delta_qry->targetList, agg_counts);
		}

		fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
		fn->agg_star = true;
		if (!new_delta_qry->groupClause && !new_delta_qry->hasAggs)
			new_delta_qry->groupClause = transformDistinctClause(NULL, &new_delta_qry->targetList, new_delta_qry->sortClause, false);

		node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

		tle = makeTargetEntry((Expr *) node,
								  list_length(new_delta_qry->targetList) + 1,
								  NULL,
								  false);
		new_delta_qry->targetList = lappend(new_delta_qry->targetList, tle);
		new_delta_qry->hasAggs = true;
	}

	if (trigdata->tg_oldtable)
	{
		RangeTblEntry *rte;
		ListCell   *lc;

		TargetEntry *tle;
		Node *node;
		FuncCall *fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);

		EphemeralNamedRelation enr =
			palloc(sizeof(EphemeralNamedRelationData));

		enr->md.name = trigdata->tg_trigger->tgoldtable;
		enr->md.reliddesc = trigdata->tg_relation->rd_id;
		enr->md.tupdesc = NULL;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(trigdata->tg_oldtable);
		enr->reldata = trigdata->tg_oldtable;
		register_ENR(queryEnv, enr);

		rte = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
		old_delta_qry->rtable = lappend(old_delta_qry->rtable, rte);

		foreach(lc, old_delta_qry->rtable)
		{
			RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);
			if (r->relid == relid)
			{
				lfirst(lc) = rte;
				break;
			}
		}

		if (query->hasAggs)
		{
			ListCell *lc;
			List *agg_counts = NIL;
			AttrNumber next_resno = list_length(query->targetList) + 1;
			Node *node;

			foreach(lc, query->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);
				TargetEntry *tle_count;

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
						fn = makeFuncCall(list_make1(makeString("sum")), NIL, -1);

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
				}

			}
			old_delta_qry->targetList = list_concat(old_delta_qry->targetList, agg_counts);
		}

		fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
		fn->agg_star = true;

		if (!old_delta_qry->groupClause && !old_delta_qry->hasAggs)
			old_delta_qry->groupClause = transformDistinctClause(NULL, &old_delta_qry->targetList, old_delta_qry->sortClause, false);

		node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);
		tle = makeTargetEntry((Expr *) node,
								  list_length(old_delta_qry->targetList) + 1,
								  NULL,
								  false);
		old_delta_qry->targetList = lappend(old_delta_qry->targetList, tle);
		old_delta_qry->hasAggs = true;
	}


	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

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

	tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
	relpersistence = RELPERSISTENCE_TEMP;

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	if (trigdata->tg_newtable)
	{
		OIDDelta_new = make_new_heap(matviewOid, tableSpace, relpersistence,
									 ExclusiveLock);
		LockRelationOid(OIDDelta_new, AccessExclusiveLock);
		dest_new = CreateTransientRelDestReceiver(OIDDelta_new);
	}
	if (trigdata->tg_oldtable)
	{
		if (trigdata->tg_newtable)
			OIDDelta_old = make_new_heap(OIDDelta_new, tableSpace, relpersistence,
										 ExclusiveLock);
		else
			OIDDelta_old = make_new_heap(matviewOid, tableSpace, relpersistence,
										 ExclusiveLock);
		LockRelationOid(OIDDelta_old, AccessExclusiveLock);
		dest_old = CreateTransientRelDestReceiver(OIDDelta_old);
	}

	/*
	 * Now lock down security-restricted operations.
	 */
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	/* Generate the data. */
	if (trigdata->tg_newtable)
		refresh_matview_datafill(dest_new, new_delta_qry, queryEnv, NULL);
	if (trigdata->tg_oldtable)
		refresh_matview_datafill(dest_old, old_delta_qry, queryEnv, NULL);

	PG_TRY();
	{
		apply_delta(matviewOid, OIDDelta_new, OIDDelta_old,
					query, relowner, save_sec_context);
	}
	PG_CATCH();
	{
		matview_maintenance_depth = old_depth;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Pop the original snapshot. */
	PopActiveSnapshot();

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return PointerGetDatum(NULL);
}

static void
apply_delta(Oid matviewOid, Oid tempOid_new, Oid tempOid_old,
			Query *query, Oid relowner, int save_sec_context)
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
	bool 		has_min_or_max = false;
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
				appendStringInfo(&update_aggs_old,
					"%s = %s - %s",
					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", resname),
					quote_qualified_identifier("t", resname)
				);
				appendStringInfo(&update_aggs_new,
					"%s = %s + %s",
					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", resname),
					quote_qualified_identifier("diff", resname)
				);

				appendStringInfo(&diff_aggs_buf, "%s",
					quote_qualified_identifier("diff", resname)
				);
			}
			else if (!strcmp(aggname, "sum"))
			{
				appendStringInfo(&update_aggs_old,
					"%s = CASE WHEN %s = %s THEN NULL ELSE "
						"COALESCE(%s,0) - COALESCE(%s, 0) END, "
					"%s = %s - %s",
					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier("mv", resname),
					quote_qualified_identifier("t", resname),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_"))
				);
				appendStringInfo(&update_aggs_new,
					"%s = CASE WHEN %s = 0 AND %s = 0 THEN NULL ELSE "
						"COALESCE(%s,0) + COALESCE(%s, 0) END, "
					"%s = %s + %s",
					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier("mv", resname),
					quote_qualified_identifier("diff", resname),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
				);

				appendStringInfo(&diff_aggs_buf, "%s, %s",
					quote_qualified_identifier("diff", resname),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
				);
			}
			else if (!strcmp(aggname, "avg"))
			{
				appendStringInfo(&update_aggs_old,
					"%s = CASE WHEN %s = %s THEN NULL ELSE "
						"(COALESCE(%s,0) - COALESCE(%s, 0))::%s / (%s - %s) END, "
					"%s = COALESCE(%s,0) - COALESCE(%s, 0), "
					"%s = %s - %s",

					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier("mv", makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_sum",resname,"_")),
					aggtype,
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_sum",resname,"_")),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_"))
				);
				appendStringInfo(&update_aggs_new,
					"%s = CASE WHEN %s = 0 AND %s = 0 THEN NULL ELSE "
						"(COALESCE(%s,0)+ COALESCE(%s, 0))::%s / (%s + %s) END, "
					"%s = COALESCE(%s,0) + COALESCE(%s, 0), "
					"%s = %s + %s",

					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier("mv", makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_sum",resname,"_")),
					aggtype,
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_sum",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_sum",resname,"_")),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
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

				appendStringInfo(&update_aggs_old,
					"%s = CASE WHEN %s = %s THEN NULL ELSE "
						"%s END, "
					"%s = %s - %s",
					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_")),

					quote_qualified_identifier("mv", resname),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("t", makeObjectName("__ivm_count",resname,"_"))
				);
				appendStringInfo(&update_aggs_new,
					"%s = CASE WHEN %s = 0 AND %s = 0 THEN NULL ELSE "
						"%s(%s,%s) END, "
					"%s = %s + %s",
					quote_qualified_identifier(NULL, resname),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_")),

					is_min ? "least" : "greatest",
					quote_qualified_identifier("mv", resname),
					quote_qualified_identifier("diff", resname),

					quote_qualified_identifier(NULL, makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("mv", makeObjectName("__ivm_count",resname,"_")),
					quote_qualified_identifier("diff", makeObjectName("__ivm_count",resname,"_"))
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

	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	OpenMatViewIncrementalMaintenance();

	if (query->hasAggs)
	{
		if (tempname_old)
		{
			resetStringInfo(&querybuf);
			appendStringInfo(&querybuf,
							"WITH t AS ("
							"  SELECT diff.__ivm_count__, "
							"         %s,"
							"         (diff.__ivm_count__ = mv.__ivm_count__) AS for_dlt, "
							"         mv.ctid"
							"  FROM %s AS mv, %s AS diff WHERE (%s) = (%s)"
							"), updt AS ("
							"  UPDATE %s AS mv SET __ivm_count__ = mv.__ivm_count__ - t.__ivm_count__,"
							"                      %s"
							"  FROM t WHERE mv.ctid = t.ctid AND NOT for_dlt"
							"   %s"
							"), dlt AS ("
							"  DELETE FROM %s AS mv USING t WHERE mv.ctid = t.ctid AND for_dlt "
							") %s",
							diff_aggs_buf.data,
							matviewname, tempname_old, mv_gkeys_buf.data, diff_gkeys_buf.data,
							matviewname, update_aggs_old.data,
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

					plan = get_plan_for_set_min_max(matviewname, min_or_max_buf.data,
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
							"  UPDATE %s AS mv SET __ivm_count__ = mv.__ivm_count__ + diff.__ivm_count__"
							", %s "
							"  FROM %s AS diff WHERE (%s) = (%s)"
							"  RETURNING %s"
							") INSERT INTO %s (SELECT * FROM %s AS diff WHERE (%s) NOT IN (SELECT %s FROM updt));",
							matviewname, update_aggs_new.data, tempname_new,
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
							"  SELECT diff.__ivm_count__, (diff.__ivm_count__ = mv.__ivm_count__) AS for_dlt, mv.ctid"
							"  FROM %s AS mv, %s AS diff WHERE (%s) = (%s)"
							"), updt AS ("
							"  UPDATE %s AS mv SET __ivm_count__ = mv.__ivm_count__ - t.__ivm_count__"
							"  FROM t WHERE mv.ctid = t.ctid AND NOT for_dlt"
							") DELETE FROM %s AS mv USING t WHERE mv.ctid = t.ctid AND for_dlt;",
							matviewname, tempname_old, mvatts_buf.data, diffatts_buf.data, matviewname, matviewname);
			if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
				elog(ERROR, "SPI_exec failed: %s", querybuf.data);
		}
		if (tempname_new)
		{
			resetStringInfo(&querybuf);
			appendStringInfo(&querybuf,
							"WITH updt AS ("
							"  UPDATE %s AS mv SET __ivm_count__ = mv.__ivm_count__ + diff.__ivm_count__"
							"  FROM %s AS diff WHERE (%s) = (%s)"
							"  RETURNING %s"
							") INSERT INTO %s (SELECT * FROM %s AS diff WHERE (%s) NOT IN (SELECT * FROM updt));",
							matviewname, tempname_new, mvatts_buf.data, diffatts_buf.data, diffatts_buf.data, matviewname, tempname_new, diffatts_buf.data);
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

	/* Clean up temp tables. */
	if (OidIsValid(tempOid_new))
	{
		resetStringInfo(&querybuf);
		appendStringInfo(&querybuf, "DROP TABLE %s", tempname_new);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}
	if (OidIsValid(tempOid_old))
	{
		resetStringInfo(&querybuf);
		appendStringInfo(&querybuf, "DROP TABLE %s", tempname_old);
		if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}


static SPIPlanPtr
get_plan_for_recalc_min_max(Oid matviewOid, const char *min_max_list,
							const char *group_keys, int nkeys, Oid *keyTypes, bool with_group)
{
	StringInfoData	str;
	char	*viewdef;
	int 	i;

	/* XXX: This doesn't work well since all matviews share only one cache.
	 *
	if (plan_for_recalc_min_max)
		return plan_for_recalc_min_max;
	*/

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

	plan_for_recalc_min_max = SPI_prepare(str.data, (with_group ? nkeys : 0), (with_group ? keyTypes : NULL));
	SPI_keepplan(plan_for_recalc_min_max);

	return plan_for_recalc_min_max;
}

static SPIPlanPtr
get_plan_for_set_min_max(char *matviewname, const char *min_max_list,
						  int num_min_max, Oid *valTypes, bool with_group)
{
	StringInfoData str;
	int 	i;

	/* XXX: This doesn't work well since all matviews share only one cache.
	 *
	if (plan_for_set_min_max)
		return plan_for_set_min_max;
	*/	

	initStringInfo(&str);
	appendStringInfo(&str, "UPDATE %s AS mv SET (%s) = (",
		matviewname, min_max_list);

	for (i = 1; i <= num_min_max; i++)
		appendStringInfo(&str, "%s$%d", (i==1 ? "" : ", "), i);

	appendStringInfo(&str, ")");

	if (with_group)
		appendStringInfo(&str, " WHERE ctid = $%d", num_min_max + 1);

	plan_for_set_min_max = SPI_prepare(str.data, num_min_max + (with_group ? 1 : 0), valTypes);
	SPI_keepplan(plan_for_set_min_max);

	return plan_for_set_min_max;
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

/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"


extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern void SetMatViewIVMState(Relation relation, bool newstate);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
										ParamListInfo params, char *completionTag);

extern DestReceiver *CreateTransientRelDestReceiver(Oid oid);

extern bool MatViewIncrementalMaintenanceIsEnabled(void);

extern Datum IVM_immediate_before(PG_FUNCTION_ARGS);
extern Datum IVM_immediate_maintenance(PG_FUNCTION_ARGS);
extern void AtAbort_IVM(void);

extern Query* rewrite_query_for_exists_subquery(Query *query, Node *node);
extern char* getIVMColumnName(RangeTblEntry *rte, char *str);

#endif							/* MATVIEW_H */

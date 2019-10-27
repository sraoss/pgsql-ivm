/*-------------------------------------------------------------------------
 *
 * createas.h
 *	  prototypes for createas.c.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/createas.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CREATEAS_H
#define CREATEAS_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/queryenvironment.h"


extern ObjectAddress ExecCreateTableAs(CreateTableAsStmt *stmt, const char *queryString,
									   ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag);

extern int	GetIntoRelEFlags(IntoClause *intoClause);

extern DestReceiver *CreateIntoRelDestReceiver(IntoClause *intoClause);

extern RangeTblEntry *convert_EXISTS_sublink_to_lateral_join(Query *query);

#endif							/* CREATEAS_H */

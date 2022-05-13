/*-------------------------------------------------------------------------
 *
 * createas.h
 *	  prototypes for createas.c.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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
#include "nodes/pathnodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "utils/queryenvironment.h"


extern ObjectAddress ExecCreateTableAs(ParseState *pstate, CreateTableAsStmt *stmt,
									   ParamListInfo params, QueryEnvironment *queryEnv,
									   QueryCompletion *qc);

extern void CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid, bool is_create);
extern void CreateIndexOnIMMV(Query *query, Relation matviewRel, bool is_create);

extern Query *rewriteQueryForIMMV(Query *query, List *colNames);
extern void makeIvmAggColumn(ParseState *pstate, Aggref *aggref, char *resname, AttrNumber *next_resno, List **aggs);

extern int	GetIntoRelEFlags(IntoClause *intoClause);

extern DestReceiver *CreateIntoRelDestReceiver(IntoClause *intoClause);

extern bool CreateTableAsRelExists(CreateTableAsStmt *ctas);

#endif							/* CREATEAS_H */

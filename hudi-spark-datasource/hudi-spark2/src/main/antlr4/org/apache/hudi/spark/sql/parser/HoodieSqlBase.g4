/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */

grammar HoodieSqlBase;

import SqlBase;

singleStatement
    : statement EOF
    ;

statement
    : mergeInto                                                        #mergeIntoTable
    | .*?                                                              #passThrough
    ;


mergeInto
    : MERGE INTO target=tableIdentifier tableAlias
      USING (source=tableIdentifier | '(' subquery = query ')') tableAlias
      mergeCondition
      matchedClauses*
      notMatchedClause*
    ;

mergeCondition
    : ON condition=booleanExpression
    ;

matchedClauses
    : deleteClause
    | updateClause
    ;

notMatchedClause
    : insertClause
    ;

deleteClause
    : WHEN MATCHED (AND deleteCond=booleanExpression)? THEN deleteAction
    | WHEN deleteCond=booleanExpression THEN deleteAction
    ;

updateClause
    : WHEN MATCHED (AND updateCond=booleanExpression)? THEN updateAction
    | WHEN updateCond=booleanExpression THEN updateAction
    ;

insertClause
    : WHEN NOT MATCHED (AND insertCond=booleanExpression)? THEN insertAction
    | WHEN insertCond=booleanExpression THEN insertAction
    ;
deleteAction
    : DELETE
    ;

updateAction
    : UPDATE SET ASTERISK
    | UPDATE SET assignmentList
    ;

insertAction
    : INSERT ASTERISK
    | INSERT '(' columns=qualifiedNameList ')' VALUES '(' expression (',' expression)* ')'
    ;

assignmentList
    : assignment (',' assignment)*
    ;

assignment
    : key=qualifiedName EQ value=expression
    ;
qualifiedNameList
    : qualifiedName (',' qualifiedName)*
    ;

PRIMARY: 'PRIMARY';
KEY: 'KEY';
MERGE: 'MERGE';
MATCHED: 'MATCHED';
UPDATE: 'UPDATE';

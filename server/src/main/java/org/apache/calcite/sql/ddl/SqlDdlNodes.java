/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.ddl;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {
  private SqlDdlNodes() {}

  /** Creates a CREATE SCHEMA. */
  public static SqlCreateSchema createSchema(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name) {
    return new SqlCreateSchema(pos, replace, ifNotExists, name);
  }

  /** Creates a CREATE FOREIGN SCHEMA. */
  public static SqlCreateForeignSchema createForeignSchema(SqlParserPos pos,
      boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode type,
      SqlNode library, SqlNodeList optionList) {
    return new SqlCreateForeignSchema(pos, replace, ifNotExists, name, type,
        library, optionList);
  }

  /** Creates a CREATE TABLE. */
  public static SqlCreateTable createTable(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList,
      SqlNode query) {
    return new SqlCreateTable(pos, replace, ifNotExists, name, columnList,
        query);
  }

  /** Creates a CREATE VIEW. */
  public static SqlCreateView createView(SqlParserPos pos, boolean replace,
      boolean materialized, SqlIdentifier name, SqlNodeList columnList,
      SqlNode query) {
    return new SqlCreateView(pos, replace, materialized, name, columnList,
        query);
  }

  /** Creates a DROP [ FOREIGN ] SCHEMA. */
  public static SqlDropSchema dropSchema(SqlParserPos pos, boolean foreign,
      boolean ifExists, SqlIdentifier name) {
    return new SqlDropSchema(pos, foreign, ifExists, name);
  }

  /** Creates a DROP TABLE. */
  public static SqlDropTable dropTable(SqlParserPos pos, boolean ifExists,
      SqlIdentifier name) {
    return new SqlDropTable(pos, ifExists, name);
  }

  /** Creates a DROP VIEW. */
  public static SqlDropView dropView(SqlParserPos pos, boolean materialized,
      boolean ifExists, SqlIdentifier name) {
    return new SqlDropView(pos, materialized, ifExists, name);
  }

  /** Creates a column declaration. */
  public static SqlNode column(SqlParserPos pos, SqlIdentifier name,
      SqlDataTypeSpec dataType, SqlNode expression, ColumnStrategy strategy) {
    return new SqlColumnDeclaration(pos, name, dataType, expression, strategy);
  }

  /** Creates a CHECK constraint. */
  public static SqlNode check(SqlParserPos pos, SqlIdentifier name,
      SqlNode expression) {
    return new SqlCheckConstraint(pos, name, expression);
  }

  /** Creates a UNIQUE constraint. */
  public static SqlKeyConstraint unique(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList) {
    return new SqlKeyConstraint(pos, name, columnList);
  }

  /** Creates a PRIMARY KEY constraint. */
  public static SqlKeyConstraint primary(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList) {
    return new SqlKeyConstraint(pos, name, columnList) {
      @Override public SqlOperator getOperator() {
        return PRIMARY;
      }
    };
  }

  /** Returns the schema in which to create an object. */
  static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context,
      boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema = mutable ? context.getMutableRootSchema()
        : context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    return Pair.of(schema, name);
  }
}

// End SqlDdlNodes.java

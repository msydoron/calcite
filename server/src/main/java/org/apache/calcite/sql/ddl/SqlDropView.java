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
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code DROP VIEW} statement.
 */
public class SqlDropView extends SqlDrop
    implements SqlExecutableStatement {
  private final boolean materialized;
  private final SqlIdentifier name;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP VIEW", SqlKind.DROP_VIEW);

  /** Creates a SqlDropView. */
  SqlDropView(SqlParserPos pos, boolean materialized, boolean ifExists,
      SqlIdentifier name) {
    super(OPERATOR, pos, ifExists);
    this.materialized = materialized;
    this.name = name;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.<SqlNode>of(name);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    if (materialized) {
      writer.keyword("MATERIALIZED");
    }
    writer.keyword("VIEW");
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public void execute(CalcitePrepare.Context context) {
    final List<String> path = context.getDefaultSchemaPath();
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    // Not quite right: removes any other functions with the same name
    final boolean existed = schema.removeFunction(name.getSimple());
    if (!existed && !ifExists) {
      throw SqlUtil.newContextException(name.getParserPosition(),
          RESOURCE.viewNotFound(name.getSimple()));
    }
  }
}

// End SqlDropView.java

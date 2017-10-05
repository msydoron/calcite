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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>SqlDialect</code> implementation for the Oracle database.
 */
public class OracleSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new OracleSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.ORACLE)
          .withIdentifierQuoteString("\""));

  /** Creates an OracleSqlDialect. */
  public OracleSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      SqlUtil.unparseFunctionSyntax(OracleSqlOperatorTable.SUBSTR, writer, call);
    } else {
      switch (call.getKind()) {
      case FLOOR:
        if (call.operandCount() != 2) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
          return;
        }

        final SqlLiteral timeUnitNode = call.operand(1);
        final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

        SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
            timeUnitNode.getParserPosition());
        SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
        break;

      case VALUES:
        List<SqlSelect> list = new ArrayList<>();
        for (SqlNode node : call.getOperandList()) {
          final SqlCall row = (SqlCall) node;
          final List<SqlNode> values = row.getOperandList();
          final List<SqlNode> values2 = new ArrayList<>();
          for (Pair<SqlNode, String> value
              : Pair.zip(values, Arrays.asList("a", "b"))) {
            values2.add(
                SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, value.left,
                    new SqlIdentifier(value.right, SqlParserPos.ZERO)));
          }
          list.add(
              new SqlSelect(SqlParserPos.ZERO, null,
                  new SqlNodeList(values2, SqlParserPos.ZERO),
                  new SqlIdentifier("DUAL", SqlParserPos.ZERO), null, null,
                  null, null, null, null, null));
        }
        SqlNode node;
        if (list.size() == 1) {
          node = list.get(0);
        } else {
          node = SqlStdOperatorTable.UNION_ALL.createCall(
              new SqlNodeList(list, SqlParserPos.ZERO));
        }
        node.unparse(writer, 0, 0);
        break;

      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }
}

// End OracleSqlDialect.java

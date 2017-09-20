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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlFloorFunction;

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
    super(context
        .withSequenceSupport(OracleSequenceSupport.INSTANCE));
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case SUBSTRING:
      SqlUtil.unparseFunctionSyntax(OracleSqlOperatorTable.SUBSTR, writer,
          call);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit =
          timeUnitNode.getValueAs(TimeUnitRange.class);

      SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call,
          timeUnit.name(), timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
      break;

    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }
}

// End OracleSqlDialect.java

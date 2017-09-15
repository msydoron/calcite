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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNode query;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  /** Creates a SqlCreateTable. */
  SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = name;
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }

  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
    final ImmutableList.Builder<ColumnDef> b = ImmutableList.builder();
    for (Ord<SqlNode> c : Ord.zip(columnList)) {
      assert c.e instanceof SqlColumnDeclaration;
      final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
      final RelDataType type = d.dataType.deriveType(typeFactory, true);
      builder.add(d.name.getSimple(), type);
      if (d.strategy != ColumnStrategy.VIRTUAL) {
        storedBuilder.add(d.name.getSimple(), type);
      }
      b.add(ColumnDef.of(d.expression, type, d.strategy));
    }
    final List<ColumnDef> columns = b.build();
    final InitializerExpressionFactory ief =
        new NullInitializerExpressionFactory() {
          @Override public ColumnStrategy generationStrategy(RelOptTable table,
              int iColumn) {
            return columns.get(iColumn).strategy;
          }

          @Override public RexNode newColumnDefaultValue(RelOptTable table,
              int iColumn, InitializerContext context) {
            final ColumnDef c = columns.get(iColumn);
            if (c.expr != null) {
              return context.convertExpression(c.expr);
            }
            return super.newColumnDefaultValue(table, iColumn, context);
          }
        };
    if (pair.left.plus().getTable(pair.right) == null) {
      // Table does not exist. Create it.
      pair.left.add(pair.right,
          new MutableArrayTable(pair.right,
              RelDataTypeImpl.proto(storedBuilder.build()),
              RelDataTypeImpl.proto(builder.build()), ief));
    } else if (!ifNotExists) {
      // Table exists. They did not specify IF NOT EXISTS, so give error.
      throw SqlUtil.newContextException(pos,
          RESOURCE.tableExists(pair.right));
    }
  }

  /** Column definition. */
  private static class ColumnDef {
    final SqlNode expr;
    final RelDataType type;
    final ColumnStrategy strategy;

    private ColumnDef(SqlNode expr, RelDataType type,
        ColumnStrategy strategy) {
      this.expr = expr;
      this.type = type;
      this.strategy = Preconditions.checkNotNull(strategy);
      Preconditions.checkArgument(
          strategy == ColumnStrategy.NULLABLE
              || strategy == ColumnStrategy.NOT_NULLABLE
              || expr != null);
    }

    static ColumnDef of(SqlNode expr, RelDataType type,
        ColumnStrategy strategy) {
      return new ColumnDef(expr, type, strategy);
    }
  }

  /** Abstract base class for implementations of {@link ModifiableTable}. */
  abstract static class AbstractModifiableTable
      extends AbstractTable implements ModifiableTable {
    AbstractModifiableTable(String tableName) {
      super();
    }

    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      return LogicalTableModify.create(table, catalogReader, child, operation,
          updateColumnList, sourceExpressionList, flattened);
    }
  }

  /** Table backed by a Java list. */
  static class MutableArrayTable extends AbstractModifiableTable
      implements Wrapper {
    final List rows = new ArrayList();
    private final RelProtoDataType protoStoredRowType;
    private final RelProtoDataType protoRowType;
    private final InitializerExpressionFactory initializerExpressionFactory;

    /** Creates a MutableArrayTable.
     *
     * @param name Name of table within its schema
     * @param protoStoredRowType Prototype of row type of stored columns (all
     *     columns except virtual columns)
     * @param protoRowType Prototype of row type (all columns)
     * @param initializerExpressionFactory How columns are populated
     */
    MutableArrayTable(String name, RelProtoDataType protoStoredRowType,
        RelProtoDataType protoRowType,
        InitializerExpressionFactory initializerExpressionFactory) {
      super(name);
      this.protoStoredRowType = Preconditions.checkNotNull(protoStoredRowType);
      this.protoRowType = Preconditions.checkNotNull(protoRowType);
      this.initializerExpressionFactory =
          Preconditions.checkNotNull(initializerExpressionFactory);
    }

    public Collection getModifiableCollection() {
      return rows;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        public Enumerator<T> enumerator() {
          //noinspection unchecked
          return (Enumerator<T>) Linq4j.enumerator(rows);
        }
      };
    }

    public Type getElementType() {
      return Object[].class;
    }

    public Expression getExpression(SchemaPlus schema, String tableName,
        Class clazz) {
      return Schemas.tableExpression(schema, getElementType(),
          tableName, clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override public <C> C unwrap(Class<C> aClass) {
      if (aClass.isInstance(initializerExpressionFactory)) {
        return aClass.cast(initializerExpressionFactory);
      }
      return super.unwrap(aClass);
    }
  }
}

// End SqlCreateTable.java

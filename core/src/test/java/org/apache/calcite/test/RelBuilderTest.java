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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link RelBuilder}.
 *
 * <p>Tasks:</p>
 * <ol>
 *   <li>Add RelBuilder.scan(List&lt;String&gt;)</li>
 *   <li>Add RelBuilder.scan(Table)</li>
 *   <li>Test that {@link RelBuilder#filter} does not create a filter if the
 *   predicates optimize to true</li>
 *   <li>Test that {@link RelBuilder#filter} DOES create a filter if the
 *   predicates optimize to false. (Creating an empty Values seems too
 *   devious.)</li>
 *   <li>Test that {@link RelBuilder#scan} throws good error if table not
 *   found</li>
 *   <li>Test that {@link RelBuilder#scan} obeys case-sensitivity</li>
 *   <li>Test that {@link RelBuilder#join(JoinRelType, String...)} obeys
 *   case-sensitivity</li>
 *   <li>Test RelBuilder with alternative factories</li>
 *   <li>Test that {@link RelBuilder#field(String)} obeys case-sensitivity</li>
 *   <li>Test case-insensitive unique field names</li>
 *   <li>Test that an alias created using
 *      {@link RelBuilder#alias(RexNode, String)} is removed if not a top-level
 *      project</li>
 *   <li>{@link RelBuilder#aggregate} with grouping sets</li>
 *   <li>{@link RelBuilder#aggregateCall} with filter</li>
 *   <li>Add call to create {@link TableFunctionScan}</li>
 *   <li>Add call to create {@link Window}</li>
 *   <li>Add call to create {@link TableModify}</li>
 *   <li>Add call to create {@link Exchange}</li>
 *   <li>Add call to create {@link Correlate}</li>
 *   <li>Add call to create {@link AggregateCall} with filter</li>
 * </ol>
 */
public class RelBuilderTest {
  /** Creates a config based on the "scott" schema. */
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /** Converts a relational expression to a string with linux line-endings. */
  private String str(RelNode r) {
    return Util.toLinux(RelOptUtil.toString(r));
  }

  @Test public void testScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    final RelNode root =
        RelBuilder.create(config().build())
            .scan("EMP")
            .build();
    assertThat(str(root),
        is("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testScanFilterTrue() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE TRUE
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(builder.literal(true))
            .build();
    assertThat(str(root),
        is("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testScanFilterEquals() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"), builder.literal(20)))
            .build();
    assertThat(str(root),
        is("LogicalFilter(condition=[=($7, 20)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testScanFilterOr() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE (deptno = 20 OR comm IS NULL) AND mgr IS NOT NULL
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.OR,
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.isNull(builder.field(6))),
                builder.isNotNull(builder.field(3)))
            .build();
    assertThat(str(root),
        is("LogicalFilter(condition=[AND(OR(=($7, 20), IS NULL($6)), IS NOT NULL($3))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testBadFieldName() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RexInputRef ref = builder.scan("EMP").field("deptno");
      fail("expected error, got " + ref);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("field [deptno] not found; input fields are: [EMPNO, ENAME, JOB, "
              + "MGR, HIREDATE, SAL, COMM, DEPTNO]"));
    }
  }

  @Test public void testBadFieldOrdinal() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RexInputRef ref = builder.scan("DEPT").field(20);
      fail("expected error, got " + ref);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("field ordinal [20] out of range; "
                  + "input fields are: [DEPTNO, DNAME, LOC]"));
    }
  }

  @Test public void testBadType() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      builder.scan("EMP");
      RexNode call = builder.call(SqlStdOperatorTable.PLUS,
          builder.field(1),
          builder.field(3));
      fail("expected error, got " + call);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("cannot derive type: +; "
              + "operands: [$1: VARCHAR(10), $3: SMALLINT]"));
    }
  }

  @Test public void testProject() {
    // Equivalent SQL:
    //   SELECT deptno, CAST(comm AS SMALLINT) AS comm, 20 AS $f2,
    //     comm AS comm3, comm AS c
    //   FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
    // Note: CAST(COMM) gets the COMM alias because it occurs first
    // Note: AS(COMM, C) becomes just $6
    assertThat(str(root),
        is(
            "LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL], $f2=[20], COMM3=[$6], C=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  /** Tests each method that creates a scalar expression. */
  @Test public void testProject2() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.or(
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.and(builder.literal(false),
                        builder.equals(builder.field("DEPTNO"),
                            builder.literal(10)),
                        builder.and(builder.isNull(builder.field(6)),
                            builder.not(builder.isNotNull(builder.field(7))))),
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(30))),
                builder.alias(builder.isNull(builder.field(2)), "n2"),
                builder.alias(builder.isNotNull(builder.field(3)), "nn2"),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
    assertThat(str(root),
        is("LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL],"
                + " $f2=[OR(=($7, 20), AND(false, =($7, 10), IS NULL($6),"
                + " NOT(IS NOT NULL($7))), =($7, 30))], n2=[IS NULL($2)],"
                + " nn2=[IS NOT NULL($3)], $f5=[20], COMM6=[$6], C=[$6])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testProjectIdentity() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.fields(Mappings.bijection(Arrays.asList(0, 1, 2))))
            .build();
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testProjectLeadingEdge() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.fields(Mappings.bijection(Arrays.asList(0, 1, 2))))
            .build();
    final String expected = "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testPermute() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .permute(Mappings.bijection(Arrays.asList(1, 2, 0)))
            .build();
    final String expected = "LogicalProject(JOB=[$2], EMPNO=[$0], ENAME=[$1])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testConvert() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("b", SqlTypeName.VARCHAR, 10)
            .add("c", SqlTypeName.VARCHAR, 10)
            .build();
    RelNode root =
        builder.scan("DEPT")
            .convert(rowType, false)
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[CAST($0):BIGINT NOT NULL], DNAME=[CAST($1):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], LOC=[CAST($2):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testConvertRename() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("b", SqlTypeName.VARCHAR, 10)
            .add("c", SqlTypeName.VARCHAR, 10)
            .build();
    RelNode root =
        builder.scan("DEPT")
            .convert(rowType, true)
            .build();
    final String expected = ""
        + "LogicalProject(a=[CAST($0):BIGINT NOT NULL], b=[CAST($1):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], c=[CAST($2):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testAggregate() {
    // Equivalent SQL:
    //   SELECT COUNT(DISTINCT deptno) AS c
    //   FROM emp
    //   GROUP BY ()
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, true, null,
                    "C", builder.field("DEPTNO")))
            .build();
    assertThat(str(root),
        is("LogicalAggregate(group=[{}], C=[COUNT(DISTINCT $7)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testAggregate2() {
    // Equivalent SQL:
    //   SELECT COUNT(*) AS c, SUM(mgr + 1) AS s
    //   FROM emp
    //   GROUP BY ename, hiredate + mgr
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field(1),
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field(4),
                        builder.field(3)),
                    builder.field(1)),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false, null,
                    "C"),
                builder.aggregateCall(SqlStdOperatorTable.SUM, false, null, "S",
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(3),
                        builder.literal(1))))
            .build();
    assertThat(str(root),
        is(""
            + "LogicalAggregate(group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testAggregateFilter() {
    // Equivalent SQL:
    //   SELECT deptno, COUNT(*) FILTER (WHERE empno > 100) AS c
    //   FROM emp
    //   GROUP BY ROLLUP(deptno)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(ImmutableBitSet.of(7),
                    ImmutableList.of(ImmutableBitSet.of(7),
                        ImmutableBitSet.of())),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false,
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field("EMPNO"), builder.literal(100)), "C"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{7}], groups=[[{7}, {}]], indicator=[true], C=[COUNT() FILTER $8])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[>($0, 100)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testAggregateGroupingKeyOutOfRangeFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(ImmutableBitSet.of(17), null))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("out of bounds: {17}"));
    }
  }

  @Test public void testAggregateGroupingSetNotSubsetFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(
                  builder.groupKey(ImmutableBitSet.of(7),
                      ImmutableList.of(ImmutableBitSet.of(4),
                          ImmutableBitSet.of())))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("group set element [$4] must be a subset of group key"));
    }
  }

  @Test public void testAggregateGroupingSetDuplicateIgnored() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(ImmutableBitSet.of(7, 6),
                    ImmutableList.of(ImmutableBitSet.of(7),
                        ImmutableBitSet.of(6),
                        ImmutableBitSet.of(7))))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{6, 7}], groups=[[{6}, {7}]], indicator=[true])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testDistinct() {
    // Equivalent SQL:
    //   SELECT DISTINCT *
    //   FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .distinct()
            .build();
    assertThat(str(root),
        is("LogicalAggregate(group=[{0, 1, 2}])\n"
                + "  LogicalTableScan(table=[[scott, DEPT]])\n"));
  }

  @Test public void testUnion() {
    // Equivalent SQL:
    //   SELECT deptno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .union(true)
            .build();
    assertThat(str(root),
        is("LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testUnion3() {
    // Equivalent SQL:
    //   SELECT deptno FROM dept
    //   UNION ALL
    //   SELECT empno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .union(true, 3)
            .build();
    assertThat(str(root),
        is("LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testUnion1() {
    // Equivalent SQL:
    //   SELECT deptno FROM dept
    //   UNION ALL
    //   SELECT empno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .union(true, 1)
            .build();
    assertThat(str(root),
        is("LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testIntersect() {
    // Equivalent SQL:
    //   SELECT empno FROM emp
    //   WHERE deptno = 20
    //   INTERSECT
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .intersect(false)
            .build();
    assertThat(str(root),
        is("LogicalIntersect(all=[false])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testIntersect3() {
    // Equivalent SQL:
    //   SELECT deptno FROM dept
    //   INTERSECT ALL
    //   SELECT empno FROM emp
    //   INTERSECT ALL
    //   SELECT deptno FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .intersect(true, 3)
            .build();
    assertThat(str(root),
        is("LogicalIntersect(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testExcept() {
    // Equivalent SQL:
    //   SELECT empno FROM emp
    //   WHERE deptno = 20
    //   MINUS
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .minus(false)
            .build();
    assertThat(str(root),
        is("LogicalMinus(all=[false])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testJoin() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT * FROM emp WHERE comm IS NULL)
    //   JOIN dept ON emp.deptno = dept.deptno
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")))
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $0)], joinType=[inner])\n"
        + "  LogicalFilter(condition=[IS NULL($6)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  /** Same as {@link #testJoin} using USING. */
  @Test public void testJoinUsing() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root2 =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .scan("DEPT")
            .join(JoinRelType.INNER, "DEPTNO")
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $0)], joinType=[inner])\n"
        + "  LogicalFilter(condition=[IS NULL($6)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root2), is(expected));
  }

  @Test public void testJoin2() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   LEFT JOIN dept ON emp.deptno = dept.deptno AND emp.empno = 123
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "EMPNO"),
                    builder.literal(123)))
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[AND(=($7, $0), =($0, 123))], joinType=[left])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testJoinCartesian() {
    // Equivalent SQL:
    //   SELECT * emp CROSS JOIN dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .build();
    final String expected =
        "LogicalJoin(condition=[true], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testCorrelationFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP");
    final RelOptCluster cluster = builder.peek().getCluster();
    final CorrelationId id = cluster.createCorrel();
    final RexNode v =
        builder.getRexBuilder().makeCorrel(builder.peek().getRowType(), id);
    try {
      builder.filter(builder.equals(builder.field(0), v))
          .scan("DEPT")
          .join(JoinRelType.INNER, builder.literal(true), ImmutableSet.of(id));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString("variable $cor0 must not be used by left input to correlation"));
    }
  }

  @Test public void testAlias() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp AS e, dept
    //   WHERE e.deptno = dept.deptno
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .scan("DEPT")
            .join(JoinRelType.LEFT)
            .filter(
                builder.equals(builder.field("e", "DEPTNO"),
                    builder.field("DEPT", "DEPTNO")))
            .project(builder.field("e", "ENAME"),
                builder.field("DEPT", "DNAME"))
            .build();
    final String expected = "LogicalProject(ENAME=[$1], DNAME=[$9])\n"
        + "  LogicalFilter(condition=[=($7, $8)])\n"
        + "    LogicalJoin(condition=[true], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
    final RelDataTypeField field = root.getRowType().getFieldList().get(1);
    assertThat(field.getName(), is("DNAME"));
    assertThat(field.getType().isNullable(), is(true));
  }

  @Test public void testAlias2() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp AS e, emp as m, dept
    //   WHERE e.deptno = dept.deptno
    //   AND m.empno = e.mgr
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .scan("EMP")
            .as("m")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .filter(
                builder.equals(builder.field("e", "DEPTNO"),
                    builder.field("DEPT", "DEPTNO")),
                builder.equals(builder.field("m", "EMPNO"),
                    builder.field("e", "MGR")))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[AND(=($7, $16), =($8, $3))])\n"
        + "  LogicalJoin(condition=[true], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalJoin(condition=[true], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testValues() {
    // Equivalent SQL:
    //   VALUES (true, 1), (false, -50) AS t(a, b)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    final String expected =
        "LogicalValues(tuples=[[{ true, 1 }, { false, -50 }]])\n";
    assertThat(str(root), is(expected));
    final String expectedType =
        "RecordType(BOOLEAN NOT NULL a, INTEGER NOT NULL b) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  /** Tests creating Values with some field names and some values null. */
  @Test public void testValuesNullable() {
    // Equivalent SQL:
    //   VALUES (null, 1, 'abc'), (false, null, 'longer string')
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", null, "c"},
            null, 1, "abc",
            false, null, "longer string").build();
    final String expected =
        "LogicalValues(tuples=[[{ null, 1, 'abc' }, { false, null, 'longer string' }]])\n";
    assertThat(str(root), is(expected));
    final String expectedType =
        "RecordType(BOOLEAN a, INTEGER expr$1, CHAR(13) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL c) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test public void testValuesBadNullFieldNames() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values((String[]) null, "a", "b");
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadNoFields() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[0], 1, 2, 3);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadNoValues() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[]{"a", "b"});
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadOddMultiple() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[] {"a", "b"}, 1, 2, 3, 4, 5);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadAllNull() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root =
          builder.values(new String[] {"a", "b"}, null, null, 1, null);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("All values of field 'b' are null; cannot deduce type"));
    }
  }

  @Test public void testValuesAllNull() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("a", SqlTypeName.VARCHAR, 10)
            .build();
    RelNode root =
        builder.values(rowType, null, null, 1, null).build();
    final String expected =
        "LogicalValues(tuples=[[{ null, null }, { 1, null }]])\n";
    assertThat(str(root), is(expected));
    final String expectedType =
        "RecordType(BIGINT NOT NULL a, VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL a) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test public void testSort() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY 3. 1 DESC
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.field(2), builder.desc(builder.field(0)))
            .build();
    final String expected =
        "LogicalSort(sort0=[$2], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));

    // same result using ordinals
    final RelNode root2 =
        builder.scan("EMP")
            .sort(2, -1)
            .build();
    assertThat(str(root2), is(expected));
  }

  @Test public void testSortByExpression() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY ename ASC NULLS LAST, hiredate + mgr DESC NULLS FIRST
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.nullsLast(builder.desc(builder.field(1))),
                builder.nullsFirst(
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(4),
                        builder.field(3))))
            .build();
    final String expected =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$1], sort1=[$8], dir0=[DESC-nulls-last], dir1=[ASC-nulls-first])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   OFFSET 2 FETCH 10
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .limit(2, 10)
            .build();
    final String expected =
        "LogicalSort(offset=[2], fetch=[10])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  @Test public void testSortLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY deptno DESC FETCH 10
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortLimit(-1, 10, builder.desc(builder.field("DEPTNO")))
            .build();
    final String expected =
        "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[10])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));
  }

  /** Tests that a sort on a field followed by a limit gives the same
   * effect as calling sortLimit.
   *
   * <p>In general a relational operator cannot rely on the order of its input,
   * but it is reasonable to merge sort and limit if they were created by
   * consecutive builder operations. And clients such as Piglet rely on it. */
  @Test public void testSortThenLimit() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.desc(builder.field("DEPTNO")))
            .limit(-1, 10)
            .build();
    final String expected = ""
        + "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[10])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(expected));

    final RelNode root2 =
        builder.scan("EMP")
            .sortLimit(-1, 10, builder.desc(builder.field("DEPTNO")))
            .build();
    assertThat(str(root2), is(expected));
  }

  /** Tests that a sort on an expression followed by a limit gives the same
   * effect as calling sortLimit. */
  @Test public void testSortExpThenLimit() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("DEPT")
            .sort(
                builder.desc(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("DEPTNO"), builder.literal(1))))
            .limit(3, 10)
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "  LogicalSort(sort0=[$3], dir0=[DESC], offset=[3], fetch=[10])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], $f3=[+($0, 1)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(expected));

    final RelNode root2 =
        builder.scan("DEPT")
            .sortLimit(3, 10,
                builder.desc(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("DEPTNO"), builder.literal(1))))
            .build();
    assertThat(str(root2), is(expected));
  }

  /** Tests {@link org.apache.calcite.tools.RelRunner} for a VALUES query. */
  @Test public void testRunValues() throws Exception {
    // Equivalent SQL:
    //   VALUES (true, 1), (false, -50) AS t(a, b)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    try (final PreparedStatement preparedStatement = RelRunners.run(root)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = "a=true; b=1\n"
          + "a=false; b=-50\n";
      assertThat(s, is(result));
    }
  }

  /** Tests {@link org.apache.calcite.tools.RelRunner} for a table scan + filter
   * query. */
  @Test public void testRun() throws Exception {
    // Equivalent SQL:
    //   SELECT * FROM EMP WHERE DEPTNO = 20
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"), builder.literal(20)))
            .build();

    // Note that because the table has been resolved in the RelNode tree
    // we do not need to supply a "schema" as context to the runner.
    try (final PreparedStatement preparedStatement = RelRunners.run(root)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = ""
          + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00; COMM=null; DEPTNO=20\n";
      assertThat(s, is(result));
    }
  }
}

// End RelBuilderTest.java

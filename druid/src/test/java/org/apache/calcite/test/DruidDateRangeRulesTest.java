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

import org.apache.calcite.adapter.druid.DruidDateTimeUtils;
import org.apache.calcite.adapter.druid.LocalInterval;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.RexImplicationCheckerTest.Fixture;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.RangeSet;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link DateRangeRules} algorithms. */
public class DruidDateRangeRulesTest {

  @Test public void testExtractYearAndMonthFromDateColumn() {
    final Fixture2 f = new Fixture2();
    // AND(>=($8, 2014-01-01), <($8, 2015-01-01), >=($8, 2014-06-01), <($8, 2014-07-01))
    checkDateRange(f,
        f.and(f.eq(f.exYear, f.literal(2014)), f.eq(f.exMonth, f.literal(6))),
        is("[2014-06-01T00:00:00.000/2014-07-01T00:00:00.000]"));
  }

  @Test public void testExtractYearAndDayFromDateColumn() {
    final Fixture2 f = new Fixture2();
    // AND(AND(>=($8, 2010-01-01), <($8, 2011-01-01)),
    //     OR(AND(>=($8, 2010-01-31), <($8, 2010-02-01)),
    //        AND(>=($8, 2010-03-31), <($8, 2010-04-01)),
    //        AND(>=($8, 2010-05-31), <($8, 2010-06-01)),
    //        AND(>=($8, 2010-07-31), <($8, 2010-08-01)),
    //        AND(>=($8, 2010-08-31), <($8, 2010-09-01)),
    //        AND(>=($8, 2010-10-31), <($8, 2010-11-01)),
    //        AND(>=($8, 2010-12-31), <($8, 2011-01-01))))
    checkDateRange(f,
        f.and(f.eq(f.exYear, f.literal(2010)), f.eq(f.exDay, f.literal(31))),
        is("[2010-01-31T00:00:00.000/2010-02-01T00:00:00.000, "
            + "2010-03-31T00:00:00.000/2010-04-01T00:00:00.000, "
            + "2010-05-31T00:00:00.000/2010-06-01T00:00:00.000, "
            + "2010-07-31T00:00:00.000/2010-08-01T00:00:00.000, "
            + "2010-08-31T00:00:00.000/2010-09-01T00:00:00.000, "
            + "2010-10-31T00:00:00.000/2010-11-01T00:00:00.000, "
            + "2010-12-31T00:00:00.000/2011-01-01T00:00:00.000]"));
  }

  @Test public void testExtractYearMonthDayFromDateColumn() {
    final Fixture2 f = new Fixture2();
    // AND(>=($8, 2011-01-01),"
    //     AND(>=($8, 2011-01-01), <($8, 2020-01-01)),
    //     OR(AND(>=($8, 2011-02-01), <($8, 2011-03-01)),
    //        AND(>=($8, 2012-02-01), <($8, 2012-03-01)),
    //        AND(>=($8, 2013-02-01), <($8, 2013-03-01)),
    //        AND(>=($8, 2014-02-01), <($8, 2014-03-01)),
    //        AND(>=($8, 2015-02-01), <($8, 2015-03-01)),
    //        AND(>=($8, 2016-02-01), <($8, 2016-03-01)),
    //        AND(>=($8, 2017-02-01), <($8, 2017-03-01)),
    //        AND(>=($8, 2018-02-01), <($8, 2018-03-01)),
    //        AND(>=($8, 2019-02-01), <($8, 2019-03-01))),
    //     OR(AND(>=($8, 2012-02-29), <($8, 2012-03-01)),
    //        AND(>=($8, 2016-02-29), <($8, 2016-03-01))))
    checkDateRange(f,
        f.and(f.gt(f.exYear, f.literal(2010)), f.lt(f.exYear, f.literal(2020)),
            f.eq(f.exMonth, f.literal(2)), f.eq(f.exDay, f.literal(29))),
        is("[2012-02-29T00:00:00.000/2012-03-01T00:00:00.000, "
            + "2016-02-29T00:00:00.000/2016-03-01T00:00:00.000]"));
  }

  @Test public void testExtractYearMonthDayFromTimestampColumn() {
    final Fixture2 f = new Fixture2();
    // AND(>=($9, 2011-01-01),
    //     AND(>=($9, 2011-01-01), <($9, 2020-01-01)),
    //     OR(AND(>=($9, 2011-02-01), <($9, 2011-03-01)),
    //        AND(>=($9, 2012-02-01), <($9, 2012-03-01)),
    //        AND(>=($9, 2013-02-01), <($9, 2013-03-01)),
    //        AND(>=($9, 2014-02-01), <($9, 2014-03-01)),
    //        AND(>=($9, 2015-02-01), <($9, 2015-03-01)),
    //        AND(>=($9, 2016-02-01), <($9, 2016-03-01)),
    //        AND(>=($9, 2017-02-01), <($9, 2017-03-01)),
    //        AND(>=($9, 2018-02-01), <($9, 2018-03-01)),
    //        AND(>=($9, 2019-02-01), <($9, 2019-03-01))),
    //     OR(AND(>=($9, 2012-02-29), <($9, 2012-03-01)),"
    //        AND(>=($9, 2016-02-29), <($9, 2016-03-01))))
    checkDateRange(f,
        f.and(f.gt(f.exYear, f.literal(2010)),
            f.lt(f.exYear, f.literal(2020)),
            f.eq(f.exMonth, f.literal(2)), f.eq(f.exDay, f.literal(29))),
        is("[2012-02-29T00:00:00.000/2012-03-01T00:00:00.000, "
            + "2016-02-29T00:00:00.000/2016-03-01T00:00:00.000]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1738">[CALCITE-1738]
   * Push CAST of literals to Druid</a>. */
  @Test public void testFilterWithCast() {
    final Fixture2 f = new Fixture2();
    final Calendar c = Util.calendar();
    c.clear();
    c.set(2010, Calendar.JANUARY, 1);
    final TimestampString from = TimestampString.fromCalendarFields(c);
    c.clear();
    c.set(2011, Calendar.JANUARY, 1);
    final TimestampString to = TimestampString.fromCalendarFields(c);

    // dt >= 2010-01-01 AND dt < 2011-01-01
    checkDateRangeNoSimplify(f,
        f.and(
            f.ge(f.dt, f.cast(f.timeStampDataType, f.timestampLiteral(from))),
            f.lt(f.dt, f.cast(f.timeStampDataType, f.timestampLiteral(to)))),
        is("[2010-01-01T00:00:00.000/2011-01-01T00:00:00.000]"));
  }

  // For testFilterWithCast we need to no simplify the expression, which would
  // remove the CAST, in order to match the way expressions are presented when
  // HiveRexExecutorImpl is used in Hive
  private void checkDateRangeNoSimplify(Fixture f, RexNode e,
      Matcher<String> intervalMatcher) {
    final Map<String, RangeSet<Calendar>> operandRanges = new HashMap<>();
    // We rely on the collection being sorted (so YEAR comes before MONTH
    // before HOUR) and unique. A predicate on MONTH is not useful if there is
    // no predicate on YEAR. Then when we apply the predicate on DAY it doesn't
    // generate hundreds of ranges we'll later throw away.
    final List<TimeUnitRange> timeUnits =
        Ordering.natural().sortedCopy(DateRangeRules.extractTimeUnits(e));
    for (TimeUnitRange timeUnit : timeUnits) {
      e = e.accept(
          new DateRangeRules.ExtractShuttle(f.rexBuilder, timeUnit,
              operandRanges));
    }
    final List<LocalInterval> intervals =
        DruidDateTimeUtils.createInterval(e, "UTC");
    assertThat(intervals, notNullValue());
    assertThat(intervals.toString(), intervalMatcher);
  }

  private void checkDateRange(Fixture f, RexNode e, Matcher<String> intervalMatcher) {
    final Map<String, RangeSet<Calendar>> operandRanges = new HashMap<>();
    // We rely on the collection being sorted (so YEAR comes before MONTH
    // before HOUR) and unique. A predicate on MONTH is not useful if there is
    // no predicate on YEAR. Then when we apply the predicate on DAY it doesn't
    // generate hundreds of ranges we'll later throw away.
    final List<TimeUnitRange> timeUnits =
        Ordering.natural().sortedCopy(DateRangeRules.extractTimeUnits(e));
    for (TimeUnitRange timeUnit : timeUnits) {
      e = e.accept(
          new DateRangeRules.ExtractShuttle(f.rexBuilder, timeUnit,
              operandRanges));
    }
    final RexNode e2 = f.simplify.simplify(e);
    List<LocalInterval> intervals =
        DruidDateTimeUtils.createInterval(e2, "UTC");
    if (intervals == null) {
      throw new AssertionError("null interval");
    }
    assertThat(intervals.toString(), intervalMatcher);
  }

  /** Common expressions across tests. */
  private static class Fixture2 extends Fixture {
    private final RexNode exYear;
    private final RexNode exMonth;
    private final RexNode exDay;

    Fixture2() {
      exYear = rexBuilder.makeCall(SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.YEAR), ts));
      exMonth = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.MONTH), ts));
      exDay = rexBuilder.makeCall(intRelDataType,
          SqlStdOperatorTable.EXTRACT,
          ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.DAY), ts));
    }
  }
}

// End DruidDateRangeRulesTest.java

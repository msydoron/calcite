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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import javax.sql.DataSource;

/** Test case for a RelBuilder bug */
public class RelBuilderBugTest {

  /** Tests a bug view. */
  @Test public void testRelBuilderProjectBug() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection = DriverManager
        .getConnection("jdbc:calcite:", new Properties());
    CalciteConnection conn = connection.unwrap(CalciteConnection.class);

    DataSource source = JdbcSchema.dataSource("jdbc:hsqldb:res:scott",
        "org.hsqldb.jdbcDriver", "SCOTT", "TIGER");
    JdbcSchema schema = JdbcSchema.create(conn.getRootSchema(),
        "SCOTT", source, null, null);
    conn.getRootSchema().add("SCOTT", schema);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(conn.getRootSchema())
        .build();
    RelBuilder builder = RelBuilder.create(config);
    RelRunner runner = conn.unwrap(RelRunner.class);

    RelNode values = builder.values(new String[]{"a", "b"}, "X", 1, "Y", 2)
        .project(builder.field("a")).build();

    System.out.println(values.getRowType());
    // run this *before* the scott query and it works fine
//     runner.prepare(values).executeQuery();

    runner.prepare(builder.scan("SCOTT", "EMP").build()).executeQuery();
    builder.clear();

    System.out.println(RelOptUtil.toString(values));
    // running this after the scott query causes the exception
    RelRunner runner2 = conn.unwrap(RelRunner.class);
    runner2.prepare(values).executeQuery();
  }
}

// End RelBuilderBugTest.java

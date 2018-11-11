/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;


/**
 * Tests {@code INSERT .. ON CONFLICT} introduced in PostgreSQL 9.5.
 */
@RunWith(Parameterized.class)
public class VxUpsertTest extends VxBaseTest4 {
  public VxUpsertTest(BinaryMode binaryMode, ReWriteBatchedInserts rewrite) {
    setBinaryMode(binaryMode);
    setReWriteBatchedInserts(rewrite);
  }

  @Parameterized.Parameters(name = "binary = {0}, reWriteBatchedInserts = {1}")
  public static Iterable<Object[]> data() {
    Collection<Object[]> ids = new ArrayList<Object[]>();
    for (BinaryMode binaryMode : BinaryMode.values()) {
      for (ReWriteBatchedInserts rewrite : ReWriteBatchedInserts.values()) {
        ids.add(new Object[]{binaryMode, rewrite});
      }
    }
    return ids;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeMinimumServerVersion(ServerVersion.v9_5);

    VxTestUtil.createTempTable(con, "test_statement", "i int primary key, t varchar(5)");
    VxStatement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO test_statement(i, t) VALUES (42, '42')").get();
    VxTestUtil.closeQuietly(stmt);
  }

  @Override
  public void tearDown() throws SQLException {
    try {
      VxTestUtil.dropTable(con, "test_statement");
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    super.tearDown();
  }

  protected int executeUpdate(String sql) throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement ps = con.prepareStatement(sql);
    int count = ps.executeUpdate().get();
    ps.close();
    return count;
  }

  @Test
  public void testUpsertDoNothingConflict() throws SQLException, InterruptedException, ExecutionException {
    int count = executeUpdate(
        "INSERT INTO test_statement(i, t) VALUES (42, '42') ON CONFLICT DO NOTHING");
    assertEquals("insert on CONFLICT DO NOTHING should report 0 modified rows on CONFLICT",
        0, count);
  }

  @Test
  public void testUpsertDoNothingNoConflict() throws SQLException, InterruptedException, ExecutionException {
    int count = executeUpdate(
        "INSERT INTO test_statement(i, t) VALUES (43, '43') ON CONFLICT DO NOTHING");
    assertEquals("insert on conflict DO NOTHING should report 1 modified row on plain insert",
        1, count);
  }

  @Test
  public void testUpsertDoUpdateConflict() throws SQLException, InterruptedException, ExecutionException {
    int count = executeUpdate(
        "INSERT INTO test_statement(i, t) VALUES (42, '42') ON CONFLICT(i) DO UPDATE SET t='43'");
    assertEquals("insert ON CONFLICT DO UPDATE should report 1 modified row on CONFLICT",
        1, count);
  }

  @Test
  public void testUpsertDoUpdateNoConflict() throws SQLException, InterruptedException, ExecutionException {
    int count = executeUpdate(
        "INSERT INTO test_statement(i, t) VALUES (43, '43') ON CONFLICT(i) DO UPDATE SET t='43'");
    assertEquals("insert on conflict do update should report 1 modified row on plain insert",
        1, count);
  }

  @Test
  public void testSingleValuedUpsertBatch() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement ps = null;
    try {
      ps = con.prepareStatement(
          "insert into test_statement(i, t) values (?,?) ON CONFLICT (i) DO NOTHING");
      ps.setInt(1, 50);
      ps.setString(2, "50");
      ps.addBatch();
      ps.setInt(1, 53);
      ps.setString(2, "53");
      ps.addBatch();
      int[] actual = ps.executeBatch().get();
      BatchExecuteTest.assertSimpleInsertBatch(2, actual);
    } finally {
      VxTestUtil.closeQuietly(ps);
    }
  }

  @Test
  public void testMultiValuedUpsertBatch() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement ps = null;
    try {
      ps = con.prepareStatement(
          "insert into test_statement(i, t) values (?,?),(?,?) ON CONFLICT (i) DO NOTHING");
      ps.setInt(1, 50);
      ps.setString(2, "50");
      ps.setInt(3, 51);
      ps.setString(4, "51");
      ps.addBatch();
      ps.setInt(1, 52);
      ps.setString(2, "52");
      ps.setInt(3, 53);
      ps.setString(4, "53");
      ps.addBatch();
      int[] actual = ps.executeBatch().get();

      BatchExecuteTest.assertBatchResult("2 batched rows, 2-values each", new int[]{2, 2}, actual);

      VxStatement st = con.createStatement();
      VxResultSet rs =
          st.executeQuery("select count(*) from test_statement where i between 50 and 53").get();
      rs.next();
      Assert.assertEquals("test_statement should have 4 rows with 'i' of 50..53", 4, rs.getInt(1));
    } finally {
      VxTestUtil.closeQuietly(ps);
    }
  }

  @Test
  public void testSingleValuedUpsertUpdateBatch() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement ps = null;
    try {
      ps = con.prepareStatement(
          "insert into test_statement(i, t) values (?,?) ON CONFLICT (i) DO update set t=?");
      ps.setInt(1, 50);
      ps.setString(2, "50U");
      ps.setString(3, "50U");
      ps.addBatch();
      ps.setInt(1, 53);
      ps.setString(2, "53U");
      ps.setString(3, "53U");
      ps.addBatch();
      int[] actual = ps.executeBatch().get();
      BatchExecuteTest.assertSimpleInsertBatch(2, actual);
    } finally {
      VxTestUtil.closeQuietly(ps);
    }
  }

  @Test
  public void testSingleValuedUpsertUpdateConstantBatch() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement ps = null;
    try {
      // For reWriteBatchedInserts=YES the following is expected
      // FE=> Parse(stmt=null,query="insert into test_statement(i, t) values ($1,$2),($3,$4) ON CONFLICT (i) DO update set t='DEF'",oids={23,1043,23,1043})
      ps = con.prepareStatement(
          "insert into test_statement(i, t) values (?,?) ON CONFLICT (i) DO update set t='DEF'");
      ps.setInt(1, 50);
      ps.setString(2, "50");
      ps.addBatch();
      ps.setInt(1, 53);
      ps.setString(2, "53");
      ps.addBatch();
      int[] actual = ps.executeBatch().get();
      BatchExecuteTest.assertSimpleInsertBatch(2, actual);
    } finally {
      VxTestUtil.closeQuietly(ps);
    }
  }
}

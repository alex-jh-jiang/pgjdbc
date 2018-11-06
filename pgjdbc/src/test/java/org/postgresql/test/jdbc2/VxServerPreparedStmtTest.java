/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Assume;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

/*
 * Tests for using server side prepared statements
 */
public class VxServerPreparedStmtTest extends VxBaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();

    Assume.assumeTrue("Server-prepared statements are not supported in simple protocol, thus ignoring the tests",
        preferQueryMode != PreferQueryMode.SIMPLE);

    VxStatement stmt = con.createStatement();

    VxTestUtil.createTable(con, "testsps", "id integer, value boolean");

    stmt.executeUpdate("INSERT INTO testsps VALUES (1,'t')").get();
    stmt.executeUpdate("INSERT INTO testsps VALUES (2,'t')").get();
    stmt.executeUpdate("INSERT INTO testsps VALUES (3,'t')").get();
    stmt.executeUpdate("INSERT INTO testsps VALUES (4,'t')").get();
    stmt.executeUpdate("INSERT INTO testsps VALUES (6,'t')").get();
    stmt.executeUpdate("INSERT INTO testsps VALUES (9,'f')").get();

    stmt.close();
  }

  @Override
  public void tearDown() throws SQLException {
    try {
      VxTestUtil.dropTable(con, "testsps");
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    super.tearDown();
  }

  @Test
  public void testEmptyResults() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE id = ?");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    for (int i = 0; i < 10; ++i) {
      pstmt.setInt(1, -1);
      VxResultSet rs = pstmt.executeQuery().get();
      assertFalse(rs.next().get());
      rs.close();
    }
    pstmt.close();
  }

  @Test
  public void testPreparedExecuteCount() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("UPDATE testsps SET id = id + 44");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    int count = pstmt.executeUpdate().get();
    assertEquals(6, count);
    pstmt.close();
  }


  @Test
  public void testPreparedStatementsNoBinds() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE id = 2");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    assertTrue(((VxStatement) pstmt).isUseServerPrepare());

    // Test that basic functionality works
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    // Verify that subsequent calls still work
    rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    // Verify that using the statement still works after turning off prepares


    if (Boolean.getBoolean("org.postgresql.forceBinary")) {
      return;
    }
    ((VxStatement) pstmt).setUseServerPrepare(false);
    assertTrue(!((VxStatement) pstmt).isUseServerPrepare());

    rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    pstmt.close();
  }

  @Test
  public void testPreparedStatementsWithOneBind() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE id = ?");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    assertTrue(((VxStatement) pstmt).isUseServerPrepare());

    // Test that basic functionality works
    pstmt.setInt(1, 2);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    // Verify that subsequent calls still work
    rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    // Verify that using the statement still works after turning off prepares
    if (Boolean.getBoolean("org.postgresql.forceBinary")) {
      return;
    }

    ((VxStatement) pstmt).setUseServerPrepare(false);
    assertTrue(!((VxStatement) pstmt).isUseServerPrepare());

    pstmt.setInt(1, 9);
    rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(9, (Object)rs.getInt(1).get());
    rs.close();

    pstmt.close();
  }

  // Verify we can bind booleans-as-objects ok.
  @Test
  public void testBooleanObjectBind() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE value = ?");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    assertTrue(((VxStatement) pstmt).isUseServerPrepare());

    pstmt.setObject(1, Boolean.FALSE, java.sql.Types.BIT);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(9, (Object)rs.getInt(1).get());
    rs.close();
  }

  // Verify we can bind booleans-as-integers ok.
  @Test
  public void testBooleanIntegerBind() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE id = ?");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    assertTrue(((VxStatement) pstmt).isUseServerPrepare());

    pstmt.setObject(1, Boolean.TRUE, java.sql.Types.INTEGER);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(1, (Object)rs.getInt(1).get());
    rs.close();
  }

  // Verify we can bind booleans-as-native-types ok.
  @Test
  public void testBooleanBind() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE value = ?");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    assertTrue(((VxStatement) pstmt).isUseServerPrepare());

    pstmt.setBoolean(1, false);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(9, (Object)rs.getInt(1).get());
    rs.close();
  }

  @Test
  public void testPreparedStatementsWithBinds() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE id = ? or id = ?");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    assertTrue(((VxStatement) pstmt).isUseServerPrepare());

    // Test that basic functionality works
    // bind different datatypes
    pstmt.setInt(1, 2);
    pstmt.setLong(2, 2);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    // Verify that subsequent calls still work
    rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    rs.close();

    pstmt.close();
  }

  @Test
  public void testSPSToggle() throws Exception {
    // Verify we can toggle UseServerPrepare safely before a query is executed.
    VxPreparedStatement pstmt = con.prepareStatement("SELECT * FROM testsps WHERE id = 2");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    ((VxStatement) pstmt).setUseServerPrepare(false);
  }

  @Test
  public void testBytea() throws Exception {
    // Verify we can use setBytes() with a server-prepared update.
    try {
      VxTestUtil.createTable(con, "testsps_bytea", "data bytea");

      VxPreparedStatement pstmt = con.prepareStatement("INSERT INTO testsps_bytea(data) VALUES (?)");
      ((VxStatement) pstmt).setUseServerPrepare(true);
      pstmt.setBytes(1, new byte[100]);
      pstmt.executeUpdate();
    } finally {
      VxTestUtil.dropTable(con, "testsps_bytea");
    }
  }

  // Check statements are not transformed when they shouldn't be.
  @Test
  public void testCreateTable() throws Exception {
    // CREATE TABLE isn't supported by PREPARE; the driver should realize this and
    // still complete without error.
    VxPreparedStatement pstmt = con.prepareStatement("CREATE TABLE testsps_bad(data int)");
    ((VxStatement) pstmt).setUseServerPrepare(true);
    pstmt.executeUpdate();
    VxTestUtil.dropTable(con, "testsps_bad");
  }

  @Test
  public void testMultistatement() throws Exception {
    // Shouldn't try to PREPARE this one, if we do we get:
    // PREPARE x(int,int) AS INSERT .... $1 ; INSERT ... $2 -- syntax error
    try {
      VxTestUtil.createTable(con, "testsps_multiple", "data int");
      VxPreparedStatement pstmt = con.prepareStatement(
          "INSERT INTO testsps_multiple(data) VALUES (?); INSERT INTO testsps_multiple(data) VALUES (?)");
      ((VxStatement) pstmt).setUseServerPrepare(true);
      pstmt.setInt(1, 1);
      pstmt.setInt(2, 2);
      pstmt.executeUpdate(); // Two inserts.

      pstmt.setInt(1, 3);
      pstmt.setInt(2, 4);
      pstmt.executeUpdate(); // Two more inserts.

      VxResultSet check = con.createStatement().executeQuery("SELECT COUNT(*) FROM testsps_multiple").get();
      assertTrue(check.next().get());
      assertEquals(4, (Object)check.getInt(1).get());
    } finally {
      VxTestUtil.dropTable(con, "testsps_multiple");
    }
  }

  @Test
  public void testTypeChange() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT CAST (? AS TEXT)");
    ((VxStatement) pstmt).setUseServerPrepare(true);

    // Prepare with int parameter.
    pstmt.setInt(1, 1);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals(1, (Object)rs.getInt(1).get());
    assertTrue(!rs.next().get());

    // Change to text parameter, check it still works.
    pstmt.setString(1, "test string");
    rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertEquals("test string", rs.getString(1).get());
    assertTrue(!rs.next().get());
  }
}

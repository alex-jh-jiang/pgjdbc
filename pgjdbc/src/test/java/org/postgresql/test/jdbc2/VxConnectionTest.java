/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;

/**
 * TestCase to test the internal functionality of org.postgresql.jdbc2.Connection and it's
 * superclass.
 */
public class VxConnectionTest {
  private VxConnection con;

  // Set up the fixture for this testcase: the tables for this test.
  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con, "test_a", "imagename name,image oid,id int4");
    VxTestUtil.createTable(con, "test_c", "source text,cost money,imageid int4");

    VxTestUtil.closeDB(con);
  }

  // Tear down the fixture for this test case.
  @After
  public void tearDown() throws Exception {
    VxTestUtil.closeDB(con);

    con = VxTestUtil.openDB().get();

    VxTestUtil.dropTable(con, "test_a");
    VxTestUtil.dropTable(con, "test_c");

    VxTestUtil.closeDB(con);
  }

  /*
   * Tests the two forms of createStatement()
   */
  @Test
  public void testCreateStatement() throws Exception {
    con = VxTestUtil.openDB().get();

    // A standard Statement
    VxStatement stat = con.createStatement();
    assertNotNull(stat);
    stat.close();

    // Ask for Updateable ResultSets
    stat = con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    assertNotNull(stat);
    stat.close();
  }

  /*
   * Tests the two forms of prepareStatement()
   */
  @Test
  public void testPrepareStatement() throws Exception {
    con = VxTestUtil.openDB().get();

    String sql = "select source,cost,imageid from test_c";

    // A standard Statement
    VxPreparedStatement stat = con.prepareStatement(sql);
    assertNotNull(stat);
    stat.close();

    // Ask for Updateable ResultSets
    stat = con.prepareStatement(sql, java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    assertNotNull(stat);
    stat.close();
  }

  /*
   * Put the test for createPrepareCall here
   */
  @Test
  public void testPrepareCall() {
  }

  /*
   * Test nativeSQL
   */
  @Test
  public void testNativeSQL() throws Exception {
    // test a simple escape
    con = VxTestUtil.openDB().get();
    assertEquals("DATE '2005-01-24'", con.nativeSQL("{d '2005-01-24'}"));
  }

  /*
   * Test autoCommit (both get & set)
   */
  @Test
  public void testTransactions() throws Exception {
    con = VxTestUtil.openDB().get();
    VxStatement st;
    VxResultSet rs;

    // Turn it off
    con.setAutoCommit(false).get();
    assertTrue(!con.getAutoCommit());

    // Turn it back on
    con.setAutoCommit(true).get();
    assertTrue(con.getAutoCommit());

    // Now test commit
    st = con.createStatement();
    st.executeUpdate("insert into test_a (imagename,image,id) values ('comttest',1234,5678)").get();

    con.setAutoCommit(false).get();

    // Now update image to 9876 and commit
    st.executeUpdate("update test_a set image=9876 where id=5678").get();
    con.commit().get();
    rs = st.executeQuery("select image from test_a where id=5678").get();
    assertTrue(rs.next().get());
    assertEquals(9876, (Object)rs.getInt(1).get());
    rs.close();

    // Now try to change it but rollback
    st.executeUpdate("update test_a set image=1111 where id=5678").get();
    con.rollback().get();
    rs = st.executeQuery("select image from test_a where id=5678").get();
    assertTrue(rs.next().get());
    assertEquals(9876, (Object)rs.getInt(1).get()); // Should not change!
    rs.close();

    VxTestUtil.closeDB(con);
  }

  /*
   * Simple test to see if isClosed works.
   */
  @Test
  public void testIsClosed() throws Exception {
    con = VxTestUtil.openDB().get();

    // Should not say closed
    assertTrue(!con.isClosed());

    VxTestUtil.closeDB(con);

    // Should now say closed
    assertTrue(con.isClosed());
  }

  /*
   * Test the warnings system
   */
  @Test
  public void testWarnings() throws Exception {
    con = VxTestUtil.openDB().get();

    String testStr = "This Is OuR TeSt message";

    // The connection must be ours!
    assertTrue(con instanceof org.postgresql.PGConnection);

    // Clear any existing warnings
    con.clearWarnings();

    // Set the test warning
    con.addWarning(new SQLWarning(testStr));

    // Retrieve it
    SQLWarning warning = con.getWarnings();
    assertNotNull(warning);
    assertEquals(testStr, warning.getMessage());

    // Finally test clearWarnings() this time there must be something to delete
    con.clearWarnings();
    assertNull(con.getWarnings());

    VxTestUtil.closeDB(con);
  }

  /*
   * Transaction Isolation Levels
   */
  @Test
  public void testTransactionIsolation() throws Exception {
    con = VxTestUtil.openDB().get();

    int defaultLevel = con.getTransactionIsolation().get();

    // Begin a transaction
    con.setAutoCommit(false).get();

    // The isolation level should not have changed
    assertEquals(defaultLevel, (Object)con.getTransactionIsolation().get());

    // Now run some tests with autocommit enabled.
    con.setAutoCommit(true).get();

    assertEquals(defaultLevel, con.getTransactionIsolation());

    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
    assertEquals(java.sql.Connection.TRANSACTION_SERIALIZABLE, (Object)con.getTransactionIsolation().get());

    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
    assertEquals(java.sql.Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());

    // Test if a change of isolation level before beginning the
    // transaction affects the isolation level inside the transaction.
    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
    assertEquals(java.sql.Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
    con.setAutoCommit(false);
    assertEquals(java.sql.Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
    con.setAutoCommit(true).get();
    assertEquals(java.sql.Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
    assertEquals(java.sql.Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
    con.setAutoCommit(false);
    assertEquals(java.sql.Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
    con.commit().get();

    // Test that getTransactionIsolation() does not actually start a new txn.
    // Shouldn't start a new transaction.
    con.getTransactionIsolation().get();
    // Should be ok -- we're not in a transaction.
    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE).get();
    // Should still be ok.
    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED).get();

    // Test that we can't change isolation mid-transaction
    VxStatement stmt = con.createStatement();
    stmt.executeQuery("SELECT 1").get(); // Start transaction.
    stmt.close();

    try {
      con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE).get();
      fail("Expected an exception when changing transaction isolation mid-transaction");
    } catch (SQLException e) {
      // Ok.
    }

    con.rollback().get();
    VxTestUtil.closeDB(con);
  }

  /*
   * JDBC2 Type mappings
   */
  @Test
  public void testTypeMaps() throws Exception {
    con = VxTestUtil.openDB().get();

    // preserve the current map
    Map<String, Class<?>> oldmap = con.getTypeMap();

    // now change it for an empty one
    Map<String, Class<?>> newmap = new HashMap<String, Class<?>>();
    con.setTypeMap(newmap);
    assertEquals(newmap, con.getTypeMap());

    // restore the old one
    con.setTypeMap(oldmap);
    assertEquals(oldmap, con.getTypeMap());

    VxTestUtil.closeDB(con);
  }

  /**
   * Closing a Connection more than once is not an error.
   */
  @Test
  public void testDoubleClose() throws Exception {
    con = VxTestUtil.openDB().get();
    con.close();
    con.close();
  }
}

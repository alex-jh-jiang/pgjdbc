/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.extensions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.jdbc2.VxBaseTest4;

import org.junit.Assume;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

// SELECT 'hstore'::regtype::oid
// SELECT 'hstore[]'::regtype::oid

public class VxHStoreTest extends VxBaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeTrue("server has installed hstore", isHStoreEnabled(con));
    Assume.assumeFalse("hstore is not supported in simple protocol only mode",
        preferQueryMode == PreferQueryMode.SIMPLE);
    assumeMinimumServerVersion("hstore requires PostgreSQL 8.3+", ServerVersion.v8_3);
  }

  private static boolean isHStoreEnabled(VxConnection conn) throws InterruptedException, ExecutionException {
    try {
      VxStatement stmt = conn.createStatement();
      VxResultSet rs = stmt.executeQuery("SELECT 'a=>1'::hstore::text").get();
      rs.close();
      stmt.close();
      return true;
    } catch (SQLException sqle) {
      return false;
    }
  }

  @Test
  public void testHStoreSelect() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT 'a=>1,b=>2'::hstore");
    VxResultSet rs = pstmt.executeQuery().get();
    assertEquals(Map.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next().get());
    String str = rs.getString(1).get();
    if (!("\"a\"=>\"1\", \"b\"=>\"2\"".equals(str) || "\"b\"=>\"2\", \"a\"=>\"1\"".equals(str))) {
      fail("Expected " + "\"a\"=>\"1\", \"b\"=>\"2\"" + " but got " + str);
    }
    Map<String, String> correct = new HashMap<String, String>();
    correct.put("a", "1");
    correct.put("b", "2");
    assertEquals(correct, rs.getObject(1));
  }

  @Test
  public void testHStoreSelectNullValue() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = con.prepareStatement("SELECT 'a=>NULL'::hstore");
    VxResultSet rs = pstmt.executeQuery().get();
    assertEquals(Map.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next().get());
    assertEquals("\"a\"=>NULL", rs.getString(1));
    Map<String, Object> correct = Collections.singletonMap("a", null);
    assertEquals(correct, rs.getObject(1));
  }

  @Test
  public void testHStoreSend() throws SQLException, InterruptedException, ExecutionException {
    Map<String, Integer> correct = Collections.singletonMap("a", 1);
    VxPreparedStatement pstmt = con.prepareStatement("SELECT ?::text");
    pstmt.setObject(1, correct);
    VxResultSet rs = pstmt.executeQuery().get();
    assertEquals(String.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next().get());
    assertEquals("\"a\"=>\"1\"", rs.getString(1));
  }

  @Test
  public void testHStoreUsingPSSetObject4() throws SQLException, InterruptedException, ExecutionException {
    Map<String, Integer> correct = Collections.singletonMap("a", 1);
    VxPreparedStatement pstmt = con.prepareStatement("SELECT ?::text");
    pstmt.setObject(1, correct, Types.OTHER, -1);
    VxResultSet rs = pstmt.executeQuery().get();
    assertEquals(String.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next().get());
    assertEquals("\"a\"=>\"1\"", rs.getString(1));
  }

  @Test
  public void testHStoreSendEscaped() throws SQLException, InterruptedException, ExecutionException {
    Map<String, String> correct = Collections.singletonMap("a", "t'e\ns\"t");
    VxPreparedStatement pstmt = con.prepareStatement("SELECT ?");
    pstmt.setObject(1, correct);
    VxResultSet rs = pstmt.executeQuery().get();
    assertEquals(Map.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next().get());
    assertEquals(correct, rs.getObject(1));
    assertEquals("\"a\"=>\"t'e\ns\\\"t\"", rs.getString(1));
  }

}

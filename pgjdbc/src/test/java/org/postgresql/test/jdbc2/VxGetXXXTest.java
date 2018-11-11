/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

/*
 * Test for getObject
 */
public class VxGetXXXTest {
  private VxConnection con = null;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();
    VxTestUtil.createTempTable(con, "test_interval",
        "initial timestamp with time zone, final timestamp with time zone");
    VxPreparedStatement pstmt = con.prepareStatement("insert into test_interval values (?,?)");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_YEAR, -1);

    pstmt.setTimestamp(1, new Timestamp(cal.getTime().getTime()));
    pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
    assertEquals(1, (Object)pstmt.executeUpdate().get());
    pstmt.close();
  }

  @After
  public void tearDown() throws Exception {
    VxTestUtil.dropTable(con, "test_interval");
    con.close();
  }

  @Test
  public void testGetObject() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("select (final-initial) as diff from test_interval").get();
    while (rs.next().get()) {
      String str = rs.getString(1).get();

      assertNotNull(str);
      Object obj = rs.getObject(1);
      assertNotNull(obj);
    }
  }

  @Test
  public void testGetUDT() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("select (final-initial) as diff from test_interval").get();

    while (rs.next().get()) {
      // make this return a PGobject
      Object obj = rs.getObject(1, new HashMap<String, Class<?>>()).get();

      // it should not be an instance of PGInterval
      assertTrue(obj instanceof org.postgresql.util.PGInterval);

    }

  }

}

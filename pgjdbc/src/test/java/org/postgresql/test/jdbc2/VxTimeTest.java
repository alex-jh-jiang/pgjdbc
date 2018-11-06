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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

/*
 * Some simple tests based on problems reported by users. Hopefully these will help prevent previous
 * problems from re-occurring ;-)
 *
 */
public class VxTimeTest {
  private VxConnection con;
  private boolean testSetTime = false;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();
    VxTestUtil.createTempTable(con, "testtime", "tm time, tz time with time zone");
  }

  @After
  public void tearDown() throws Exception {
    VxTestUtil.dropTable(con, "testtime");
    VxTestUtil.closeDB(con);
  }

  private long extractMillis(long time) {
    return (time >= 0) ? (time % 1000) : (time % 1000 + 1000);
  }

  /*
   *
   * Test use of calendar
   */
  @Test
  public void testGetTimeZone() throws Exception {
    final Time midnight = new Time(0, 0, 0);
    VxStatement stmt = con.createStatement();
    Calendar cal = Calendar.getInstance();

    cal.setTimeZone(TimeZone.getTimeZone("GMT"));

    int localOffset = Calendar.getInstance().getTimeZone().getOffset(midnight.getTime());

    // set the time to midnight to make this easy
    assertEquals(1, (Object)stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'00:00:00','00:00:00'")).get());
    assertEquals(1,
        stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'00:00:00.1','00:00:00.01'")));
    assertEquals(1, (Object)stmt.executeUpdate(VxTestUtil.insertSQL("testtime",
        "CAST(CAST(now() AS timestamp without time zone) AS time),now()")).get());
    VxResultSet rs = stmt.executeQuery(VxTestUtil.selectSQL("testtime", "tm,tz")).get();
    assertNotNull(rs);
    assertTrue(rs.next().get());


    Time time = rs.getTime(1).get();
    Timestamp timestamp = rs.getTimestamp(1).get();
    assertNotNull(timestamp);

    Timestamp timestamptz = rs.getTimestamp(2).get();
    assertNotNull(timestamptz);

    assertEquals(midnight, time);

    time = (Time)rs.getTime(1, cal).get();
    assertEquals(midnight.getTime(), time.getTime() - localOffset);

    assertTrue(rs.next().get());

    time = rs.getTime(1).get();
    assertNotNull(time);
    assertEquals(100, extractMillis(time.getTime()));
    timestamp = rs.getTimestamp(1).get();
    assertNotNull(timestamp);

    assertEquals(100, extractMillis(timestamp.getTime()));

    assertEquals(100000000, timestamp.getNanos());

    Time timetz = rs.getTime(2).get();
    assertNotNull(timetz);
    assertEquals(10, extractMillis(timetz.getTime()));
    timestamptz = rs.getTimestamp(2).get();
    assertNotNull(timestamptz);
    assertEquals(10, extractMillis(timestamptz.getTime()));

    assertEquals(10000000, timestamptz.getNanos());

    assertTrue(rs.next().get());

    time = rs.getTime(1).get();
    assertNotNull(time);
    timestamp = rs.getTimestamp(1).get();
    assertNotNull(timestamp);

    timetz = rs.getTime(2).get();
    assertNotNull(timetz);
    timestamptz = rs.getTimestamp(2).get();
    assertNotNull(timestamptz);
  }

  /*
   * Tests the time methods in ResultSet
   */
  @Test
  public void testGetTime() throws SQLException {
    VxStatement stmt = con.createStatement();

    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'01:02:03'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'23:59:59'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'12:00:00'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'05:15:21'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'16:21:51'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'12:15:12'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'22:12:01'")));
    assertEquals(1, stmt.executeUpdate(VxTestUtil.insertSQL("testtime", "'08:46:44'")));


    // Fall through helper
    try {
      timeTest();
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertEquals(8, stmt.executeUpdate("DELETE FROM testtime"));
    stmt.close();
  }

  /*
   * Tests the time methods in PreparedStatement
   */
  @Test
  public void testSetTime() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement ps = con.prepareStatement(VxTestUtil.insertSQL("testtime", "?"));
    VxStatement stmt = con.createStatement();

    ps.setTime(1, makeTime(1, 2, 3));
    assertEquals(1, ps.executeUpdate());

    ps.setTime(1, makeTime(23, 59, 59));
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, Time.valueOf("12:00:00"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, Time.valueOf("05:15:21"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, Time.valueOf("16:21:51"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, Time.valueOf("12:15:12"), java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "22:12:1", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "8:46:44", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "5:1:2-03", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    ps.setObject(1, "23:59:59+11", java.sql.Types.TIME);
    assertEquals(1, ps.executeUpdate());

    // Need to let the test know this one has extra test cases.
    testSetTime = true;
    // Fall through helper
    timeTest();
    testSetTime = false;

    assertEquals(10, stmt.executeUpdate("DELETE FROM testtime"));
    stmt.close();
    ps.close();
  }

  /*
   * Helper for the TimeTests. It tests what should be in the db
   */
  private void timeTest() throws SQLException, InterruptedException, ExecutionException {
    VxStatement st = con.createStatement();
    VxResultSet rs;
    Time t;

    rs = st.executeQuery(VxTestUtil.selectSQL("testtime", "tm")).get();
    assertNotNull(rs);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(1, 2, 3), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(23, 59, 59), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(12, 0, 0), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(5, 15, 21), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(16, 21, 51), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(12, 15, 12), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(22, 12, 1), t);

    assertTrue(rs.next().get());
    t = rs.getTime(1).get();
    assertNotNull(t);
    assertEquals(makeTime(8, 46, 44), t);

    // If we're checking for timezones.
    if (testSetTime) {
      assertTrue(rs.next().get());
      t = rs.getTime(1).get();
      assertNotNull(t);
      Time tmpTime = Time.valueOf("5:1:2");
      int localOffset = Calendar.getInstance().getTimeZone().getOffset(tmpTime.getTime());
      int timeOffset = 3 * 60 * 60 * 1000;
      tmpTime.setTime(tmpTime.getTime() + timeOffset + localOffset);
      assertEquals(makeTime(tmpTime.getHours(), tmpTime.getMinutes(), tmpTime.getSeconds()), t);

      assertTrue(rs.next().get());
      t = rs.getTime(1).get();
      assertNotNull(t);
      tmpTime = Time.valueOf("23:59:59");
      localOffset = Calendar.getInstance().getTimeZone().getOffset(tmpTime.getTime());
      timeOffset = -11 * 60 * 60 * 1000;
      tmpTime.setTime(tmpTime.getTime() + timeOffset + localOffset);
      assertEquals(makeTime(tmpTime.getHours(), tmpTime.getMinutes(), tmpTime.getSeconds()), t);
    }

    assertTrue(!rs.next().get());

    rs.close();
  }

  private Time makeTime(int h, int m, int s) {
    return Time.valueOf(VxTestUtil.fix(h, 2) + ":" + VxTestUtil.fix(m, 2) + ":" + VxTestUtil.fix(s, 2));
  }
}

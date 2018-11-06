/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGTime;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

/**
 * Tests {@link PGTime} in various scenarios including setTime, setObject for both <code>time with
 * time zone</code> and <code>time without time zone</code> data types.
 */
public class VxPGTimeTest extends VxBaseTest4 {
  /**
   * The name of the test table.
   */
  private static final String TEST_TABLE = "testtime";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    VxTestUtil.createTempTable(con, TEST_TABLE, "tm time, tz time with time zone");
  }

  @Override
  public void tearDown() throws SQLException {
    try {
      VxTestUtil.dropTable(con, TEST_TABLE);
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    super.tearDown();
  }

  /**
   * Tests that adding a <code>PGInterval</code> object to a <code>PGTime</code> object when
   * performed as a casted string and object.
   *
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  @Test
  public void testTimeWithInterval() throws SQLException, InterruptedException, ExecutionException {
    assumeTrue(VxTestUtil.haveIntegerDateTimes(con));

    Calendar cal = Calendar.getInstance();
    cal.set(1970, Calendar.JANUARY, 1);

    final long now = cal.getTimeInMillis();
    verifyTimeWithInterval(new PGTime(now), new PGInterval(0, 0, 0, 1, 2, 3.14), true);
    verifyTimeWithInterval(new PGTime(now), new PGInterval(0, 0, 0, 1, 2, 3.14), false);

    verifyTimeWithInterval(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT"))),
        new PGInterval(0, 0, 0, 1, 2, 3.14), true);
    verifyTimeWithInterval(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT"))),
        new PGInterval(0, 0, 0, 1, 2, 3.14), false);

    verifyTimeWithInterval(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00"))),
        new PGInterval(0, 0, 0, 1, 2, 3.456), true);
    verifyTimeWithInterval(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00"))),
        new PGInterval(0, 0, 0, 1, 2, 3.456), false);
  }

  /**
   * Verifies that adding the given <code>PGInterval</code> object to a <code>PGTime</code> produces
   * the correct results when performed as a casted string and object.
   *
   * @param time the time to test.
   * @param interval the time interval.
   * @param useSetObject <code>true</code> if the setObject method should be used instead of
   *        setTime.
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  private void verifyTimeWithInterval(PGTime time, PGInterval interval, boolean useSetObject)
      throws SQLException, InterruptedException, ExecutionException {
    // Construct the SQL query.
    String sql;
    if (time.getCalendar() != null) {
      sql = "SELECT ?::time with time zone + ?";
    } else {
      sql = "SELECT ?::time + ?";
    }

    SimpleDateFormat sdf = createSimpleDateFormat(time);

    // Execute a query using a casted time string + PGInterval.
    VxPreparedStatement stmt = con.prepareStatement(sql);
    stmt.setString(1, sdf.format(time));
    stmt.setObject(2, interval);

    VxResultSet rs = stmt.executeQuery().get();
    assertTrue(rs.next().get());

    Time result1 = rs.getTime(1).get();
    // System.out.println(stmt + " = " + sdf.format(result1));
    stmt.close();

    // Execute a query using with PGTime + PGInterval.
    stmt = con.prepareStatement("SELECT ? + ?");
    if (useSetObject) {
      stmt.setObject(1, time);
    } else {
      stmt.setTime(1, time);
    }
    stmt.setObject(2, interval);

    rs = stmt.executeQuery().get();
    assertTrue(rs.next().get());

    Time result2 = rs.getTime(1).get();
    // System.out.println(stmt + " = " + sdf.format(result2));
    assertEquals(result1, result2);
    stmt.close();
  }

  /**
   * Tests inserting and selecting <code>PGTime</code> objects with <code>time</code> and <code>time
   * with time zone</code> columns.
   *
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  @Test
  public void testTimeInsertAndSelect() throws SQLException, InterruptedException, ExecutionException {
    Calendar cal = Calendar.getInstance();
    cal.set(1970, Calendar.JANUARY, 1);

    final long now = cal.getTimeInMillis();
    verifyInsertAndSelect(new PGTime(now), true);
    verifyInsertAndSelect(new PGTime(now), false);

    verifyInsertAndSelect(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT"))), true);
    verifyInsertAndSelect(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT"))),
        false);

    verifyInsertAndSelect(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00"))),
        true);
    verifyInsertAndSelect(new PGTime(now, Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00"))),
        false);
  }

  /**
   * Verifies that inserting the given <code>PGTime</code> as a time string and an object produces
   * the same results.
   *
   * @param time the time to test.
   * @param useSetObject <code>true</code> if the setObject method should be used instead of
   *        setTime.
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  private void verifyInsertAndSelect(PGTime time, boolean useSetObject) throws SQLException, InterruptedException, ExecutionException {
    // Construct the INSERT statement of a casted time string.
    String sql;
    if (time.getCalendar() != null) {
      sql =
          "INSERT INTO " + TEST_TABLE + " VALUES (?::time with time zone, ?::time with time zone)";
    } else {
      sql = "INSERT INTO " + TEST_TABLE + " VALUES (?::time, ?::time)";
    }

    SimpleDateFormat sdf = createSimpleDateFormat(time);

    // Insert the times as casted strings.
    VxPreparedStatement pstmt1 = con.prepareStatement(sql);
    pstmt1.setString(1, sdf.format(time));
    pstmt1.setString(2, sdf.format(time));
    assertEquals(1, (Object)pstmt1.executeUpdate().get());

    // Insert the times as PGTime objects.
    VxPreparedStatement pstmt2 = con.prepareStatement("INSERT INTO " + TEST_TABLE + " VALUES (?, ?)");

    if (useSetObject) {
      pstmt2.setObject(1, time);
      pstmt2.setObject(2, time);
    } else {
      pstmt2.setTime(1, time);
      pstmt2.setTime(2, time);
    }

    assertEquals(1, pstmt2.executeUpdate());

    // Query the values back out.
    VxStatement stmt = con.createStatement();

    VxResultSet rs = stmt.executeQuery(VxTestUtil.selectSQL(TEST_TABLE, "tm,tz")).get();
    assertNotNull(rs);

    // Read the casted string values.
    assertTrue(rs.next().get());

    Time tm1 = rs.getTime(1).get();
    Time tz1 = rs.getTime(2).get();

    // System.out.println(pstmt1 + " -> " + tm1 + ", " + sdf.format(tz1));

    // Read the PGTime values.
    assertTrue(rs.next().get());

    Time tm2 = rs.getTime(1).get();
    Time tz2 = rs.getTime(2).get();

    // System.out.println(pstmt2 + " -> " + tm2 + ", " + sdf.format(tz2));

    // Verify that the first and second versions match.
    assertEquals(tm1, tm2);
    assertEquals(tz1, tz2);

    // Clean up.
    assertEquals(2, stmt.executeUpdate("DELETE FROM " + TEST_TABLE));
    stmt.close();
    pstmt2.close();
    pstmt1.close();
  }

  /**
   * Creates a {@code SimpleDateFormat} that is appropriate for the given time.
   *
   * @param time the time object.
   * @return the new format instance.
   */
  private SimpleDateFormat createSimpleDateFormat(PGTime time) {
    String pattern = "HH:mm:ss.SSS";
    if (time.getCalendar() != null) {
      pattern += " Z";
    }

    SimpleDateFormat sdf = new SimpleDateFormat(pattern);
    if (time.getCalendar() != null) {
      sdf.setTimeZone(time.getCalendar().getTimeZone());
    }
    return sdf;
  }
}

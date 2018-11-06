/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGTimestamp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

/**
 * Tests {@link PGTimestamp} in various scenarios including setTimestamp, setObject for both
 * {@code timestamp with time zone} and {@code timestamp without time zone} data types.
 */
public class VxPGTimestampTest {
  /**
   * The name of the test table.
   */
  private static final String TEST_TABLE = "testtimestamp";

  private VxConnection con;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con, TEST_TABLE, "ts timestamp, tz timestamp with time zone");
  }

  @After
  public void tearDown() throws Exception {
    VxTestUtil.dropTable(con, TEST_TABLE);
    VxTestUtil.closeDB(con);
  }

  /**
   * Tests {@link PGTimestamp} with {@link PGInterval}.
   *
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  @Test
  public void testTimestampWithInterval() throws SQLException, InterruptedException, ExecutionException {
    assumeTrue(VxTestUtil.haveIntegerDateTimes(con));
    PGTimestamp timestamp = new PGTimestamp(System.currentTimeMillis());
    PGInterval interval = new PGInterval(0, 0, 0, 1, 2, 3.14);
    verifyTimestampWithInterval(timestamp, interval, true);
    verifyTimestampWithInterval(timestamp, interval, false);

    timestamp = new PGTimestamp(System.currentTimeMillis(),
        Calendar.getInstance(TimeZone.getTimeZone("GMT")));
    interval = new PGInterval(0, 0, 0, 1, 2, 3.14);
    verifyTimestampWithInterval(timestamp, interval, true);
    verifyTimestampWithInterval(timestamp, interval, false);

    timestamp = new PGTimestamp(System.currentTimeMillis(),
        Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00")));
    interval = new PGInterval(-3, -2, -1, 1, 2, 3.14);
    verifyTimestampWithInterval(timestamp, interval, true);
    verifyTimestampWithInterval(timestamp, interval, false);
  }

  /**
   * Executes a test with the given timestamp and interval.
   *
   * @param timestamp the timestamp under test.
   * @param interval the interval.
   * @param useSetObject indicates if setObject should be used instead of setTimestamp.
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  private void verifyTimestampWithInterval(PGTimestamp timestamp, PGInterval interval,
      boolean useSetObject) throws SQLException, InterruptedException, ExecutionException {
    // Construct the SQL query.
    String sql;
    if (timestamp.getCalendar() != null) {
      sql = "SELECT ?::timestamp with time zone + ?";
    } else {
      sql = "SELECT ?::timestamp + ?";
    }

    // Execute a query using a casted timestamp string + PGInterval.
    VxPreparedStatement ps = con.prepareStatement(sql);
    SimpleDateFormat sdf = createSimpleDateFormat(timestamp);
    final String timestampString = sdf.format(timestamp);
    ps.setString(1, timestampString);
    ps.setObject(2, interval);
    VxResultSet rs = ps.executeQuery().get();
    assertNotNull(rs);

    assertTrue(rs.next().get());
    Timestamp result1 = rs.getTimestamp(1).get();
    assertNotNull(result1);
    ps.close();

    // Execute a query as PGTimestamp + PGInterval.
    ps = con.prepareStatement("SELECT ? + ?");
    if (useSetObject) {
      ps.setObject(1, timestamp);
    } else {
      ps.setTimestamp(1, timestamp);
    }
    ps.setObject(2, interval);
    rs = ps.executeQuery().get();

    // Verify that the query produces the same results.
    assertTrue(rs.next().get());
    Timestamp result2 = rs.getTimestamp(1).get();
    assertEquals(result1, result2);
    ps.close();
  }

  /**
   * Tests inserting and selecting {@code PGTimestamp} objects with {@code timestamp} and
   * {@code timestamp with time zone} columns.
   *
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  @Test
  public void testTimeInsertAndSelect() throws SQLException, InterruptedException, ExecutionException {
    final long now = System.currentTimeMillis();
    verifyInsertAndSelect(new PGTimestamp(now), true);
    verifyInsertAndSelect(new PGTimestamp(now), false);

    verifyInsertAndSelect(new PGTimestamp(now, Calendar.getInstance(TimeZone.getTimeZone("GMT"))),
        true);
    verifyInsertAndSelect(new PGTimestamp(now, Calendar.getInstance(TimeZone.getTimeZone("GMT"))),
        false);

    verifyInsertAndSelect(
        new PGTimestamp(now, Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00"))), true);
    verifyInsertAndSelect(
        new PGTimestamp(now, Calendar.getInstance(TimeZone.getTimeZone("GMT+01:00"))), false);
  }

  /**
   * Verifies that inserting the given {@code PGTimestamp} as a timestamp string and an object
   * produces the same results.
   *
   * @param timestamp the timestamp to test.
   * @param useSetObject {@code true} if the setObject method should be used instead of
   *        setTimestamp.
   * @throws SQLException if a JDBC or database problem occurs.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  private void verifyInsertAndSelect(PGTimestamp timestamp, boolean useSetObject)
      throws SQLException, InterruptedException, ExecutionException {
    // Construct the INSERT statement of a casted timestamp string.
    String sql;
    if (timestamp.getCalendar() != null) {
      sql = "INSERT INTO " + TEST_TABLE
          + " VALUES (?::timestamp with time zone, ?::timestamp with time zone)";
    } else {
      sql = "INSERT INTO " + TEST_TABLE + " VALUES (?::timestamp, ?::timestamp)";
    }

    SimpleDateFormat sdf = createSimpleDateFormat(timestamp);

    // Insert the timestamps as casted strings.
    VxPreparedStatement pstmt1 = con.prepareStatement(sql);
    pstmt1.setString(1, sdf.format(timestamp));
    pstmt1.setString(2, sdf.format(timestamp));
    assertEquals(1, pstmt1.executeUpdate());

    // Insert the timestamps as PGTimestamp objects.
    VxPreparedStatement pstmt2 = con.prepareStatement("INSERT INTO " + TEST_TABLE + " VALUES (?, ?)");

    if (useSetObject) {
      pstmt2.setObject(1, timestamp);
      pstmt2.setObject(2, timestamp);
    } else {
      pstmt2.setTimestamp(1, timestamp);
      pstmt2.setTimestamp(2, timestamp);
    }

    assertEquals(1, pstmt2.executeUpdate());

    // Query the values back out.
    VxStatement stmt = con.createStatement();

    VxResultSet rs = stmt.executeQuery(VxTestUtil.selectSQL(TEST_TABLE, "ts,tz")).get();
    assertNotNull(rs);

    // Read the casted string values.
    assertTrue(rs.next().get());

    Timestamp ts1 = rs.getTimestamp(1).get();
    Timestamp tz1 = rs.getTimestamp(2).get();

    // System.out.println(pstmt1 + " -> " + ts1 + ", " + sdf.format(tz1));

    // Read the PGTimestamp values.
    assertTrue(rs.next().get());

    Timestamp ts2 = rs.getTimestamp(1).get();
    Timestamp tz2 = rs.getTimestamp(2).get();

    // System.out.println(pstmt2 + " -> " + ts2 + ", " + sdf.format(tz2));

    // Verify that the first and second versions match.
    assertEquals(ts1, ts2);
    assertEquals(tz1, tz2);

    // Clean up.
    assertEquals(2, stmt.executeUpdate("DELETE FROM " + TEST_TABLE));
    stmt.close();
    pstmt2.close();
    pstmt1.close();
  }

  /**
   * Creates a {@code SimpleDateFormat} that is appropriate for the given timestamp.
   *
   * @param timestamp the timestamp object.
   * @return the new format instance.
   */
  private SimpleDateFormat createSimpleDateFormat(PGTimestamp timestamp) {
    String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
    if (timestamp.getCalendar() != null) {
      pattern += " Z";
    }

    SimpleDateFormat sdf = new SimpleDateFormat(pattern);
    if (timestamp.getCalendar() != null) {
      sdf.setTimeZone(timestamp.getCalendar().getTimeZone());
    }
    return sdf;
  }
}

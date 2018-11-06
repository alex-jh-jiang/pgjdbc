/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyOut;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.PSQLState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

/**
 * @author kato@iki.fi
 */
public class VxCopyTest {
  private VxConnection con;
  private CopyManager copyAPI;
  private String copyParams;
  // 0's required to match DB output for numeric(5,2)
  private String[] origData =
      {"First Row\t1\t1.10\n",
          "Second Row\t2\t-22.20\n",
          "\\N\t\\N\t\\N\n",
          "\t4\t444.40\n"};
  private int dataRows = origData.length;

  private byte[] getData(String[] origData) {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(buf);
    for (String anOrigData : origData) {
      ps.print(anOrigData);
    }
    return buf.toByteArray();
  }

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con, "copytest", "stringvalue text, intvalue int, numvalue numeric(5,2)");

    copyAPI = ((PGConnection) con).getCopyAPI();
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v9_0)) {
      copyParams = "(FORMAT CSV, HEADER false)";
    } else {
      copyParams = "CSV";
    }
  }

  @After
  public void tearDown() throws Exception {
    VxTestUtil.closeDB(con);

    // one of the tests will render the existing connection broken,
    // so we need to drop the table on a fresh one.
    con = VxTestUtil.openDB().get();
    try {
      VxTestUtil.dropTable(con, "copytest");
    } finally {
      con.close();
    }
  }

  private int getCount() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT count(*) FROM copytest").get();
    rs.next().get();
    int result = rs.getInt(1).get();
    rs.close();
    return result;
  }

  @Test
  public void testCopyInByRow() throws SQLException, InterruptedException, ExecutionException {
    String sql = "COPY copytest FROM STDIN";
    CopyIn cp = copyAPI.copyIn(sql).get();
    for (String anOrigData : origData) {
      byte[] buf = anOrigData.getBytes();
      cp.writeToCopy(buf, 0, buf.length).get();
    }

    long count1 = cp.endCopy().get();
    long count2 = cp.getHandledRowCount();
    assertEquals(dataRows, count1);
    assertEquals(dataRows, count2);

    try {
      cp.cancelCopy().get();
    } catch (SQLException se) { // should fail with obsolete operation
      if (!PSQLState.OBJECT_NOT_IN_STATE.getState().equals(se.getSQLState())) {
        fail("should have thrown object not in state exception.");
      }
    }
    int rowCount = getCount();
    assertEquals(dataRows, rowCount);
  }

  @Test
  public void testCopyInAsOutputStream() throws SQLException, IOException, InterruptedException, ExecutionException {
    String sql = "COPY copytest FROM STDIN";
    OutputStream os = new PGCopyOutputStream((PGConnection) con, sql, 1000);
    for (String anOrigData : origData) {
      byte[] buf = anOrigData.getBytes();
      os.write(buf);
    }
    os.close();
    int rowCount = getCount();
    assertEquals(dataRows, rowCount);
  }

  @Test
  public void testCopyInFromInputStream() throws SQLException, IOException, InterruptedException, ExecutionException {
    String sql = "COPY copytest FROM STDIN";
    try {
		copyAPI.copyIn(sql, new ByteArrayInputStream(getData(origData)), 3).get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
    int rowCount = getCount();
    assertEquals(dataRows, rowCount);
  }

  @Test
  public void testCopyInFromStreamFail() throws SQLException, InterruptedException, ExecutionException {
    String sql = "COPY copytest FROM STDIN";
    try {
      copyAPI.copyIn(sql, new InputStream() {
        public int read() {
          throw new RuntimeException("COPYTEST");
        }
      }, 3).get();
    } catch (Exception e) {
      if (!e.toString().contains("COPYTEST")) {
        fail("should have failed trying to read from our bogus stream.");
      }
    }
    int rowCount = getCount();
    assertEquals(0, rowCount);
  }

  @Test
  public void testCopyInFromReader() throws SQLException, IOException, InterruptedException, ExecutionException {
    String sql = "COPY copytest FROM STDIN";
    try {
		copyAPI.copyIn(sql, new StringReader(new String(getData(origData))), 3).get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
    int rowCount = getCount();
    assertEquals(dataRows, rowCount);
  }

  @Test
  public void testSkipping() {
    String sql = "COPY copytest FROM STDIN";
    String at = "init";
    int rowCount = -1;
    int skip = 0;
    int skipChar = 1;
    try {
      while (skipChar > 0) {
        at = "buffering";
        InputStream ins = new ByteArrayInputStream(getData(origData));
        at = "skipping";
        ins.skip(skip++);
        skipChar = ins.read();
        at = "copying";
        copyAPI.copyIn(sql, ins, 3).get();
        at = "using connection after writing copy";
        rowCount = getCount();
      }
    } catch (Exception e) {
      if (!(skipChar == '\t')) {
        // error expected when field separator consumed
        fail("testSkipping at " + at + " round " + skip + ": " + e.toString());
      }
    }
    assertEquals(dataRows * (skip - 1), rowCount);
  }

  @Test
  public void testCopyOutByRow() throws SQLException, IOException, InterruptedException, ExecutionException {
    testCopyInByRow(); // ensure we have some data.
    String sql = "COPY copytest TO STDOUT";
    CopyOut cp = copyAPI.copyOut(sql).get();
    int count = 0;
    byte[] buf;
    while ((buf = cp.readFromCopy().get()) != null) {
      count++;
    }
    assertEquals(false, cp.isActive());
    assertEquals(dataRows, count);

    long rowCount = cp.getHandledRowCount();

    assertEquals(dataRows, rowCount);

    assertEquals(dataRows, getCount());
  }

  @Test
  public void testCopyOut() throws SQLException, IOException, InterruptedException, ExecutionException {
    testCopyInByRow(); // ensure we have some data.
    String sql = "COPY copytest TO STDOUT";
    ByteArrayOutputStream copydata = new ByteArrayOutputStream();
    copyAPI.copyOut(sql, copydata).get();
    assertEquals(dataRows, getCount());
    // deep comparison of data written and read
    byte[] copybytes = copydata.toByteArray();
    assertNotNull(copybytes);
    for (int i = 0, l = 0; i < origData.length; i++) {
      byte[] origBytes = origData[i].getBytes();
      assertTrue("Copy is shorter than original", copybytes.length >= l + origBytes.length);
      for (int j = 0; j < origBytes.length; j++, l++) {
        assertEquals("content changed at byte#" + j + ": " + origBytes[j] + copybytes[l],
            origBytes[j], copybytes[l]);
      }
    }
  }

  @Test
  public void testNonCopyOut() throws SQLException, IOException, InterruptedException, ExecutionException {
    String sql = "SELECT 1";
    try {
      try {
		copyAPI.copyOut(sql, new ByteArrayOutputStream()).get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
      fail("Can't use a non-copy query.");
    } catch (SQLException sqle) {
    }
    // Ensure connection still works.
    assertEquals(0, getCount());
  }

  @Test
  public void testNonCopyIn() throws SQLException, IOException, InterruptedException, ExecutionException {
    String sql = "SELECT 1";
    try {
      try {
		copyAPI.copyIn(sql, new ByteArrayInputStream(new byte[0])).get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
      fail("Can't use a non-copy query.");
    } catch (SQLException sqle) {
    }
    // Ensure connection still works.
    assertEquals(0, getCount());
  }

  @Test
  public void testStatementCopyIn() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    try {
      stmt.execute("COPY copytest FROM STDIN");
      fail("Should have failed because copy doesn't work from a Statement.");
    } catch (SQLException sqle) {
    }
    stmt.close();

    assertEquals(0, getCount());
  }

  @Test
  public void testStatementCopyOut() throws SQLException, InterruptedException, ExecutionException {
    testCopyInByRow(); // ensure we have some data.

    VxStatement stmt = con.createStatement();
    try {
      stmt.execute("COPY copytest TO STDOUT");
      fail("Should have failed because copy doesn't work from a Statement.");
    } catch (SQLException sqle) {
    }
    stmt.close();

    assertEquals(dataRows, getCount());
  }

  @Test
  public void testCopyQuery() throws SQLException, IOException, InterruptedException, ExecutionException {
    testCopyInByRow(); // ensure we have some data.

    long count = copyAPI.copyOut("COPY (SELECT generate_series(1,1000)) TO STDOUT",
        new ByteArrayOutputStream()).get();
    assertEquals(1000, count);
  }

  @Test
  public void testCopyRollback() throws SQLException, InterruptedException, ExecutionException {
    con.setAutoCommit(false);
    testCopyInByRow();
    con.rollback();
    assertEquals(0, getCount());
  }

  @Test
  public void testChangeDateStyle() throws SQLException, InterruptedException, ExecutionException {
    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
      CopyManager manager = con.unwrap(PGConnection.class).getCopyAPI();

      VxStatement stmt = con.createStatement();

      stmt.execute("SET DateStyle = 'ISO, DMY'").get();


      // I expect an SQLException
      String sql = "COPY copytest FROM STDIN with xxx " + copyParams;
      CopyIn cp = manager.copyIn(sql).get();
      for (String anOrigData : origData) {
        byte[] buf = anOrigData.getBytes();
        cp.writeToCopy(buf, 0, buf.length).get();
      }

      long count1 = cp.endCopy().get();
      long count2 = cp.getHandledRowCount();
      con.commit().get();
    } catch (SQLException ex) {

      // the with xxx is a syntax error which shoud return a state of 42601
      // if this fails the 'S' command is not being handled in the copy manager query handler
      assertEquals("42601", ex.getSQLState());
      con.rollback().get();
    }
  }

  @Test
  public void testLockReleaseOnCancelFailure() throws SQLException, InterruptedException, ExecutionException {
    if (!VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4)) {
      // pg_backend_pid() requires PostgreSQL 8.4+
      return;
    }

    // This is a fairly complex test because it is testing a
    // deadlock that only occurs when the connection to postgres
    // is broken during a copy operation. We'll start a copy
    // operation, use pg_terminate_backend to rudely break it,
    // and then cancel. The test passes if a subsequent operation
    // on the Connection object fails to deadlock.
    con.setAutoCommit(false);

    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("select pg_backend_pid()").get();
    rs.next().get();
    int pid = rs.getInt(1).get();
    rs.close();
    stmt.close();

    CopyManager manager = con.unwrap(VxConnection.class).getCopyAPI();
    CopyIn copyIn = manager.copyIn("COPY copytest FROM STDIN with " + copyParams).get();
    try {
      killConnection(pid);
      byte[] bunchOfNulls = ",,\n".getBytes();
      while (true) {
        copyIn.writeToCopy(bunchOfNulls, 0, bunchOfNulls.length).get();
      }
    } catch (SQLException e) {
      acceptIOCause(e);
    } finally {
      if (copyIn.isActive()) {
        try {
          copyIn.cancelCopy().get();
          fail("cancelCopy should have thrown an exception");
        } catch (SQLException e) {
          acceptIOCause(e);
        }
      }
    }

    // Now we'll execute rollback on another thread so that if the
    // deadlock _does_ occur the testcase doesn't just hange forever.
    Rollback rollback = new Rollback(con);
    rollback.start();
    rollback.join(1000);
    if (rollback.isAlive()) {
      fail("rollback did not terminate");
    }
    SQLException rollbackException = rollback.exception();
    if (rollbackException == null) {
      fail("rollback should have thrown an exception");
    }
    acceptIOCause(rollbackException);
  }

  private static class Rollback extends Thread {
    private final VxConnection con;
    private SQLException rollbackException;

    Rollback(VxConnection con) {
      setName("Asynchronous rollback");
      setDaemon(true);
      this.con = con;
    }

    @Override
    public void run() {
      try {
        con.rollback();
      } catch (SQLException e) {
        rollbackException = e;
      }
    }

    public SQLException exception() {
      return rollbackException;
    }
  }

  private void killConnection(int pid) throws SQLException {
    VxConnection killerCon;
    try {
      killerCon = VxTestUtil.openPrivilegedDB().get();
    } catch (Exception e) {
      fail("Unable to open secondary connection to terminate copy");
      return; // persuade Java killerCon will not be used uninitialized
    }
    try {
      VxPreparedStatement stmt = killerCon.prepareStatement("select pg_terminate_backend(?)");
      stmt.setInt(1, pid);
      stmt.execute();
    } finally {
      killerCon.close();
    }
  }

  private void acceptIOCause(SQLException e) throws SQLException {
    if (!(e.getCause() instanceof IOException)) {
      throw e;
    }
  }

}

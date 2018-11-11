/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import javax.sql.rowset.serial.SerialBlob;

/**
 * Test that oid/lob are accessible in concurrent connection, in presence of the lo_manage trigger
 * Require the lo module accessible in $libdir
 */
public class VxBlobTransactionTest {
  private VxConnection con;
  private VxConnection con2;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();
    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
    con2 = VxTestUtil.openDB().get();
    con2.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);

    VxTestUtil.createTable(con, "testblob", "id name,lo oid");

    String sql;

    /*
     * this would have to be executed using the postgres user in order to get access to a C function
     *
     */
    VxConnection privilegedCon = VxTestUtil.openPrivilegedDB().get();
    VxStatement st = privilegedCon.createStatement();
    try {
      sql =
          "CREATE OR REPLACE FUNCTION lo_manage() RETURNS pg_catalog.trigger AS '$libdir/lo' LANGUAGE C";
      st.executeUpdate(sql).get();
    } finally {
      st.close();
    }

    st = privilegedCon.createStatement();
    try {
      sql =
          "CREATE TRIGGER testblob_lomanage BEFORE UPDATE OR DELETE ON testblob FOR EACH ROW EXECUTE PROCEDURE lo_manage(lo)";
      st.executeUpdate(sql).get();
    } finally {
      st.close();
    }
    VxTestUtil.closeDB(privilegedCon);

    con.setAutoCommit(false).get();
    con2.setAutoCommit(false).get();
  }

  @After
  public void tearDown() throws Exception {
    VxTestUtil.closeDB(con2);

    con.setAutoCommit(true).get();
    try {
      VxStatement stmt = con.createStatement();
      try {
        stmt.execute("SELECT lo_unlink(lo) FROM testblob").get();
      } finally {
        try {
          stmt.close();
        } catch (Exception e) {
        }
      }
    } finally {
      VxTestUtil.dropTable(con, "testblob");
      VxTestUtil.closeDB(con);
    }
  }

  private byte[] randomData() {
    byte[] data = new byte[64 * 1024 * 8];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) (Math.random() * 256);
    }
    return data;
  }

  private byte[] readInputStream(InputStream is) throws IOException {
    byte[] result = new byte[1024];
    int readPos = 0;
    int d;
    while ((d = is.read()) != -1) {
      if (readPos == result.length) {
        result = Arrays.copyOf(result, result.length * 2);
      }
      result[readPos++] = (byte) d;
    }

    return Arrays.copyOf(result, readPos);
  }

  @Test
  public void testConcurrentReplace() throws SQLException, IOException, InterruptedException, ExecutionException {
    // Statement stmt = con.createStatement();
    // stmt.execute("INSERT INTO testblob(id,lo) VALUES ('1', lo_creat(-1))");
    // ResultSet rs = stmt.executeQuery("SELECT lo FROM testblob");
    // assertTrue(rs.next());

    VxPreparedStatement pstmt = con.prepareStatement("INSERT INTO testblob(id, lo) VALUES(?,?)");

    byte[] initialData = randomData();

    pstmt.setString(1, "testConcurrentReplace");
    pstmt.setObject(2, new SerialBlob(initialData), Types.BLOB);
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    con.commit().get();

    con2.rollback().get();

    // con2 access the blob
    VxPreparedStatement pstmt2 = con2.prepareStatement("SELECT lo FROM testblob WHERE id=?");
    pstmt2.setString(1, "testConcurrentReplace");
    VxResultSet rs2 = pstmt2.executeQuery().get();
    assertTrue(rs2.next().get());


    // con replace the blob
    byte[] newData = randomData();
    pstmt = con.prepareStatement("UPDATE testblob SET lo=? where id=?");
    pstmt.setObject(1, new SerialBlob(newData), Types.BLOB);
    pstmt.setString(2, "testConcurrentReplace");
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    // con2 read the blob content
    Blob initContentBlob = rs2.getBlob(1).get();
    byte[] initialContentReRead = readInputStream(initContentBlob.getBinaryStream());
    assertEquals(initialContentReRead.length, initialData.length);
    for (int i = 0; i < initialContentReRead.length; ++i) {
      assertEquals(initialContentReRead[i], initialData[i]);
    }


    con2.rollback().get();
    pstmt2 = con2.prepareStatement("SELECT lo FROM testblob WHERE id=?");
    pstmt2.setString(1, "testConcurrentReplace");
    rs2 = pstmt2.executeQuery().get();
    assertTrue(rs2.next().get());

    // con commit
    con.commit().get();

    initContentBlob = rs2.getBlob(1).get();
    initialContentReRead = readInputStream(initContentBlob.getBinaryStream());
    assertEquals(initialContentReRead.length, initialData.length);
    for (int i = 0; i < initialContentReRead.length; ++i) {
      assertEquals(initialContentReRead[i], initialData[i]);
    }

    con2.commit().get();
  }
}

/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.test.TestUtil;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.ExecutionException;

/**
 * Some simple tests based on problems reported by users. Hopefully these will help prevent previous
 * problems from re-occurring ;-)
 */
public class VxBlobTest {
  private static final int LOOP = 0; // LargeObject API using loop
  private static final int NATIVE_STREAM = 1; // LargeObject API using OutputStream

  private VxConnection con;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con, "testblob", "id name,lo oid");
//    con.setAutoCommit(false);
  }

  @After
  public void tearDown() throws Exception {
    con.setAutoCommit(true);
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

  @Test
  public void testSetNull() throws Exception {
    VxPreparedStatement pstmt = con.prepareStatement("INSERT INTO testblob(lo) VALUES (?)");

    pstmt.setBlob(1, (Blob) null);
    pstmt.executeUpdate().get();

    pstmt.setNull(1, Types.BLOB);
    pstmt.executeUpdate().get();

    pstmt.setObject(1, null, Types.BLOB);
    pstmt.executeUpdate().get();

    pstmt.setClob(1, (Clob) null);
    pstmt.executeUpdate().get();

    pstmt.setNull(1, Types.CLOB);
    pstmt.executeUpdate().get();

    pstmt.setObject(1, null, Types.CLOB);
    pstmt.executeUpdate().get();
  }

  @Test
  public void testSet() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    stmt.execute("INSERT INTO testblob(id,lo) VALUES ('1', lo_creat(-1))").get();
    VxResultSet rs = stmt.executeQuery("SELECT lo FROM testblob").get();
    assertTrue(rs.next().get());

    VxPreparedStatement pstmt = con.prepareStatement("INSERT INTO testblob(id, lo) VALUES(?,?)");

    Blob blob = rs.getBlob(1).get();
    pstmt.setString(1, "setObjectTypeBlob");
    pstmt.setObject(2, blob, Types.BLOB);
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    blob = rs.getBlob(1).get();
    pstmt.setString(1, "setObjectBlob");
    pstmt.setObject(2, blob);
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    blob = rs.getBlob(1).get();
    pstmt.setString(1, "setBlob");
    pstmt.setBlob(2, blob);
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    Clob clob = rs.getClob(1).get();
    pstmt.setString(1, "setObjectTypeClob");
    pstmt.setObject(2, clob, Types.CLOB);
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    clob = rs.getClob(1).get();
    pstmt.setString(1, "setObjectClob");
    pstmt.setObject(2, clob);
    assertEquals(1, (Object)pstmt.executeUpdate().get());

    clob = rs.getClob(1).get();
    pstmt.setString(1, "setClob");
    pstmt.setClob(2, clob);
    assertEquals(1, (Object)pstmt.executeUpdate().get());
  }

  /*
   * Tests one method of uploading a blob to the database
   */
  @Test
  public void testUploadBlob_LOOP() throws Exception {
    assertTrue(uploadFile("/test-file.xml", LOOP) > 0);

    // Now compare the blob & the file. Note this actually tests the
    // InputStream implementation!
    assertTrue(compareBlobsLOAPI());
    assertTrue(compareBlobs());
    assertTrue(compareClobs());
  }

  /*
   * Tests one method of uploading a blob to the database
   */
  @Test
  public void testUploadBlob_NATIVE() throws Exception {
    assertTrue(uploadFile("/test-file.xml", NATIVE_STREAM) > 0);

    // Now compare the blob & the file. Note this actually tests the
    // InputStream implementation!
    assertTrue(compareBlobs());
  }

  @Test
  public void testMarkResetStream() throws Exception {
    assertTrue(uploadFile("/test-file.xml", NATIVE_STREAM) > 0);

    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT lo FROM testblob").get();
    assertTrue(rs.next().get());

    LargeObjectManager lom = con.getLargeObjectAPI();

    long oid = rs.getLong(1).get();
    LargeObject blob = lom.open(oid).get();
    InputStream bis = blob.getInputStream();

    assertEquals('<', bis.read());
    bis.mark(4);
    assertEquals('?', bis.read());
    assertEquals('x', bis.read());
    assertEquals('m', bis.read());
    assertEquals('l', bis.read());
    bis.reset();
    assertEquals('?', bis.read());
  }


  @Test
  public void testGetBytesOffset() throws Exception {
    assertTrue(uploadFile("/test-file.xml", NATIVE_STREAM) > 0);

    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT lo FROM testblob").get();
    assertTrue(rs.next().get());

    Blob lob = rs.getBlob(1).get();
    byte[] data = lob.getBytes(2, 4);
    assertEquals(data.length, 4);
    assertEquals(data[0], '?');
    assertEquals(data[1], 'x');
    assertEquals(data[2], 'm');
    assertEquals(data[3], 'l');
  }

  @Test
  public void testMultipleStreams() throws Exception {
    assertTrue(uploadFile("/test-file.xml", NATIVE_STREAM) > 0);

    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT lo FROM testblob").get();
    assertTrue(rs.next().get());

    Blob lob = rs.getBlob(1).get();
    byte[] data = new byte[2];

    InputStream is = lob.getBinaryStream();
    assertEquals(data.length, is.read(data));
    assertEquals(data[0], '<');
    assertEquals(data[1], '?');
    is.close();

    is = lob.getBinaryStream();
    assertEquals(data.length, is.read(data));
    assertEquals(data[0], '<');
    assertEquals(data[1], '?');
    is.close();
  }

  @Test
  public void testParallelStreams() throws Exception {
    assertTrue(uploadFile("/test-file.xml", NATIVE_STREAM) > 0);

    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT lo FROM testblob").get();
    assertTrue(rs.next().get());

    Blob lob = rs.getBlob(1).get();
    InputStream is1 = lob.getBinaryStream();
    InputStream is2 = lob.getBinaryStream();

    while (true) {
      int i1 = is1.read();
      int i2 = is2.read();
      assertEquals(i1, i2);
      if (i1 == -1) {
        break;
      }
    }

    is1.close();
    is2.close();
  }

  @Test
  public void testLargeLargeObject() throws Exception {
    if (!VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v9_3)) {
      return;
    }

    VxStatement stmt = con.createStatement();
    stmt.execute("INSERT INTO testblob(id,lo) VALUES ('1', lo_creat(-1))").get();
    VxResultSet rs = stmt.executeQuery("SELECT lo FROM testblob").get();
    assertTrue(rs.next().get());

    Blob lob = rs.getBlob(1).get();
    long length = ((long) Integer.MAX_VALUE) + 1024;
    lob.truncate(length);
    assertEquals(length, lob.length());
  }

  /*
   * Helper - uploads a file into a blob using old style methods. We use this because it always
   * works, and we can use it as a base to test the new methods.
   */
  private long uploadFile(String file, int method) throws Exception {
    LargeObjectManager lom = con.getLargeObjectAPI();

    InputStream fis = getClass().getResourceAsStream(file);

    long oid = lom.createLO(LargeObjectManager.READWRITE).get();
    LargeObject blob = lom.open(oid).get();

    int s;
    int t;
    byte[] buf;
    OutputStream os;

    switch (method) {
      case LOOP:
        buf = new byte[2048];
        t = 0;
        while ((s = fis.read(buf, 0, buf.length)) > 0) {
          t += s;
          blob.write(buf, 0, s).get();
        }
        break;

      case NATIVE_STREAM:
        os = blob.getOutputStream();
        s = fis.read();
        while (s > -1) {
          os.write(s);
          s = fis.read();
        }
        os.close();
        break;

      default:
        fail("Unknown method in uploadFile");
    }

    blob.close();
    fis.close();

    // Insert into the table
    VxStatement st = con.createStatement();
    st.executeUpdate(VxTestUtil.insertSQL("testblob", "id,lo", "'" + file + "'," + oid)).get();
    con.commit().get();
    st.close();

    return oid;
  }

  /*
   * Helper - compares the blobs in a table with a local file. Note this uses the postgresql
   * specific Large Object API
   */
  private boolean compareBlobsLOAPI() throws Exception {
    boolean result = true;

    LargeObjectManager lom = ((org.postgresql.PGConnection) con).getLargeObjectAPI();

    VxStatement st = con.createStatement();
    VxResultSet rs = st.executeQuery(VxTestUtil.selectSQL("testblob", "id,lo")).get();
    assertNotNull(rs);

    while (rs.next().get()) {
      String file = rs.getString(1).get();
      long oid = rs.getLong(2).get();

      InputStream fis = getClass().getResourceAsStream(file);
      LargeObject blob = lom.open(oid).get();
      InputStream bis = blob.getInputStream();

      int f = fis.read();
      int b = bis.read();
      int c = 0;
      while (f >= 0 && b >= 0 & result) {
        result = (f == b);
        f = fis.read();
        b = bis.read();
        c++;
      }
      result = result && f == -1 && b == -1;

      if (!result) {
        fail("Large Object API Blob compare failed at " + c + " of " + blob.size().get());
      }

      blob.close();
      fis.close();
    }
    rs.close();
    st.close();

    return result;
  }

  /*
   * Helper - compares the blobs in a table with a local file. This uses the jdbc java.sql.Blob api
   */
  private boolean compareBlobs() throws Exception {
    boolean result = true;

    VxStatement st = con.createStatement();
    VxResultSet rs = st.executeQuery(VxTestUtil.selectSQL("testblob", "id,lo")).get();
    assertNotNull(rs);

    while (rs.next().get()) {
      String file = rs.getString(1).get();
      Blob blob = rs.getBlob(2).get();

      InputStream fis = getClass().getResourceAsStream(file);
      InputStream bis = blob.getBinaryStream();

      int f = fis.read();
      int b = bis.read();
      int c = 0;
      while (f >= 0 && b >= 0 & result) {
        result = (f == b);
        f = fis.read();
        b = bis.read();
        c++;
      }
      result = result && f == -1 && b == -1;

      if (!result) {
        fail("JDBC API Blob compare failed at " + c + " of " + blob.length());
      }

      bis.close();
      fis.close();
    }
    rs.close();
    st.close();

    return result;
  }

  /*
   * Helper - compares the clobs in a table with a local file.
   */
  private boolean compareClobs() throws Exception {
    boolean result = true;

    VxStatement st = con.createStatement();
    VxResultSet rs = st.executeQuery(TestUtil.selectSQL("testblob", "id,lo")).get();
    assertNotNull(rs);

    while (rs.next().get()) {
      String file = rs.getString(1).get();
      Clob clob = rs.getClob(2).get();

      InputStream fis = getClass().getResourceAsStream(file);
      InputStream bis = clob.getAsciiStream();

      int f = fis.read();
      int b = bis.read();
      int c = 0;
      while (f >= 0 && b >= 0 & result) {
        result = (f == b);
        f = fis.read();
        b = bis.read();
        c++;
      }
      result = result && f == -1 && b == -1;

      if (!result) {
        fail("Clob compare failed at " + c + " of " + clob.length());
      }

      bis.close();
      fis.close();
    }
    rs.close();
    st.close();

    return result;
  }
}

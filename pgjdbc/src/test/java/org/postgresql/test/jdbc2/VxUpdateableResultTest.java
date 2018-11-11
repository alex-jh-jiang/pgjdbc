/*
 * Copyright (c) 2001, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.PGConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

public class VxUpdateableResultTest extends VxBaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    VxTestUtil.createTable(con, "updateable",
        "id int primary key, name text, notselected text, ts timestamp with time zone, intarr int[]",
        true);
    VxTestUtil.createTable(con, "second", "id1 int primary key, name1 text");
    VxTestUtil.createTable(con, "stream", "id int primary key, asi text, chr text, bin bytea");
    VxTestUtil.createTable(con, "multicol", "id1 int not null, id2 int not null, val text");

    VxStatement st2 = con.createStatement();
    // create pk for multicol table
    st2.execute("ALTER TABLE multicol ADD CONSTRAINT multicol_pk PRIMARY KEY (id1, id2)").get();
    // put some dummy data into second
    st2.execute("insert into second values (1,'anyvalue' )").get();
    st2.close();

  }

  @Override
  public void tearDown() throws SQLException {
    try {
      VxTestUtil.dropTable(con, "updateable");
      VxTestUtil.dropTable(con, "second");
      VxTestUtil.dropTable(con, "stream");
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    super.tearDown();
  }

  @Test
  public void testDeleteRows() throws SQLException, InterruptedException, ExecutionException {
    VxStatement st = con.createStatement();
    st.executeUpdate("INSERT INTO second values (2,'two')").get();
    st.executeUpdate("INSERT INTO second values (3,'three')").get();
    st.executeUpdate("INSERT INTO second values (4,'four')").get();
    st.close();

    st = con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select id1,name1 from second order by id1").get();

    assertTrue(rs.next().get());
    assertEquals(1, (Object)rs.getInt("id1").get());
    rs.deleteRow().get();
    assertTrue(rs.isBeforeFirst());

    assertTrue(rs.next().get());
    assertTrue(rs.next().get());
    assertEquals(3, (Object)rs.getInt("id1").get());
    rs.deleteRow().get();
    assertEquals(2, (Object)rs.getInt("id1").get());

    rs.close();
    st.close();
  }


  @Test
  public void testCancelRowUpdates() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select * from second").get();

    // make sure we're dealing with the correct row.
    rs.first();
    assertEquals(1, (Object)(Object)rs.getInt(1).get());
    assertEquals("anyvalue", rs.getString(2).get());

    // update, cancel and make sure nothings changed.
    rs.updateInt(1, 99);
    rs.cancelRowUpdates();
    assertEquals(1, (Object)rs.getInt(1).get());
    assertEquals("anyvalue", rs.getString(2).get());

    // real update
    rs.updateInt(1, 999);
    rs.updateRow().get();
    assertEquals(999, (Object)rs.getInt(1).get());
    assertEquals("anyvalue", rs.getString(2).get());

    // scroll some and make sure the update is still there
    rs.beforeFirst();
    rs.next();
    assertEquals(999, (Object)rs.getInt(1).get());
    assertEquals("anyvalue", rs.getString(2).get());


    // make sure the update got to the db and the driver isn't lying to us.
    rs.close();
    rs = st.executeQuery("select * from second").get();
    rs.first();
    assertEquals(999, (Object)rs.getInt(1).get());
    assertEquals("anyvalue", rs.getString(2).get());

    rs.close();
    st.close();
  }

  private void checkPositioning(VxResultSet rs) throws SQLException, InterruptedException, ExecutionException {
    try {
      rs.getInt(1).get();
      fail("Can't use an incorrectly positioned result set.");
    } catch (SQLException sqle) {
    }

    try {
      rs.updateInt(1, 2);
      fail("Can't use an incorrectly positioned result set.");
    } catch (SQLException sqle) {
    }

    try {
      rs.updateRow().get();
      fail("Can't use an incorrectly positioned result set.");
    } catch (SQLException sqle) {
    }

    try {
      rs.deleteRow().get();
      fail("Can't use an incorrectly positioned result set.");
    } catch (SQLException sqle) {
    }
  }

  @Test
  public void testPositioning() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = stmt.executeQuery("SELECT id1,name1 FROM second").get();

    checkPositioning(rs);

    assertTrue(rs.next().get());
    rs.beforeFirst();
    checkPositioning(rs);

    rs.afterLast();
    checkPositioning(rs);

    rs.beforeFirst();
    assertTrue(rs.next().get());
    assertTrue(!rs.next().get());
    checkPositioning(rs);

    rs.afterLast();
    assertTrue(rs.previous());
    assertTrue(!rs.previous());
    checkPositioning(rs);

    rs.close();
    stmt.close();
  }

  @Test
  public void testUpdateTimestamp() throws SQLException, InterruptedException, ExecutionException {
    TimeZone origTZ = TimeZone.getDefault();
    try {
      // We choose a timezone which has a partial hour portion
      // Asia/Tehran is +3:30
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tehran"));
      Timestamp ts = Timestamp.valueOf("2006-11-20 16:17:18");

      VxStatement stmt =
          con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
      VxResultSet rs = stmt.executeQuery("SELECT id, ts FROM updateable").get();
      rs.moveToInsertRow();
      rs.updateInt(1, 1);
      rs.updateTimestamp(2, ts);
      rs.insertRow().get();
      rs.first();
      assertEquals(ts, rs.getTimestamp(2).get());
    } finally {
      TimeZone.setDefault(origTZ);
    }
  }

  @Test
  public void testUpdateStreams() throws SQLException, UnsupportedEncodingException, InterruptedException, ExecutionException {
    assumeByteaSupported();
    String string = "Hello";
    byte[] bytes = new byte[]{0, '\\', (byte) 128, (byte) 255};

    VxStatement stmt =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = stmt.executeQuery("SELECT id, asi, chr, bin FROM stream").get();

    rs.moveToInsertRow();
    rs.updateInt(1, 1);
    rs.updateAsciiStream("asi", null, 17);
    rs.updateCharacterStream("chr", null, 81);
    rs.updateBinaryStream("bin", null, 0);
    rs.insertRow().get();

    rs.moveToInsertRow();
    rs.updateInt(1, 3);
    rs.updateAsciiStream("asi", new ByteArrayInputStream(string.getBytes("US-ASCII")), 5);
    rs.updateCharacterStream("chr", new StringReader(string), 5);
    rs.updateBinaryStream("bin", new ByteArrayInputStream(bytes), bytes.length);
    rs.insertRow().get();

    rs.beforeFirst();
    rs.next();

    assertEquals(1, (Object)rs.getInt(1).get());
    assertNull(rs.getString(2).get());
    assertNull(rs.getString(3).get());
    assertNull(rs.getBytes(4));

    rs.updateInt("id", 2);
    rs.updateAsciiStream("asi", new ByteArrayInputStream(string.getBytes("US-ASCII")), 5);
    rs.updateCharacterStream("chr", new StringReader(string), 5);
    rs.updateBinaryStream("bin", new ByteArrayInputStream(bytes), bytes.length);
    rs.updateRow().get();

    assertEquals(2, (Object)rs.getInt(1).get());
    assertEquals(string, rs.getString(2).get());
    assertEquals(string, rs.getString(3).get());
    assertArrayEquals(bytes, rs.getBytes(4));

    rs.refreshRow().get();

    assertEquals(2, (Object)rs.getInt(1).get());
    assertEquals(string, rs.getString(2).get());
    assertEquals(string, rs.getString(3).get());
    assertArrayEquals(bytes, rs.getBytes(4));

    rs.next();

    assertEquals(3, (Object)rs.getInt(1).get());
    assertEquals(string, rs.getString(2).get());
    assertEquals(string, rs.getString(3).get());
    assertArrayEquals(bytes, rs.getBytes(4));

    rs.close();
    stmt.close();
  }

  @Test
  public void testZeroRowResult() throws SQLException, InterruptedException, ExecutionException {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select * from updateable WHERE 0 > 1").get();
    assertTrue(!rs.next().get());
    rs.moveToInsertRow();
    rs.moveToCurrentRow();
  }

  @Test
  public void testUpdateable() throws SQLException, InterruptedException, ExecutionException {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select * from updateable").get();
    assertNotNull(rs);
    rs.moveToInsertRow();
    rs.updateInt(1, 1);
    rs.updateString(2, "jake");
    rs.updateString(3, "avalue");
    rs.insertRow().get();
    rs.first();

    rs.updateInt("id", 2);
    rs.updateString("name", "dave");
    rs.updateRow().get();

    assertEquals(2, rs.getInt("id"));
    assertEquals("dave", rs.getString("name"));
    assertEquals("avalue", rs.getString("notselected"));

    rs.deleteRow().get();
    rs.moveToInsertRow();
    rs.updateInt("id", 3);
    rs.updateString("name", "paul");

    rs.insertRow().get();

    try {
      rs.refreshRow().get();
      fail("Can't refresh when on the insert row.");
    } catch (SQLException sqle) {
    }

    assertEquals(3, rs.getInt("id"));
    assertEquals("paul", rs.getString("name"));
    assertNull(rs.getString("notselected"));

    rs.close();

    rs = st.executeQuery("select id1, id, name, name1 from updateable, second").get();
    try {
      while (rs.next().get()) {
        rs.updateInt("id", 2);
        rs.updateString("name", "dave");
        rs.updateRow().get();
      }


      fail("should not get here, update should fail");
    } catch (SQLException ex) {
    }

    rs = st.executeQuery("select oid,* from updateable").get();
    assertTrue(rs.first());
    rs.updateInt("id", 3);
    rs.updateString("name", "dave3");
    rs.updateRow();
    assertEquals(3, rs.getInt("id"));
    assertEquals("dave3", rs.getString("name"));

    rs.moveToInsertRow();
    rs.updateInt("id", 4);
    rs.updateString("name", "dave4");

    rs.insertRow().get();
    rs.updateInt("id", 5);
    rs.updateString("name", "dave5");
    rs.insertRow().get();

    rs.moveToCurrentRow();
    assertEquals(3, rs.getInt("id"));
    assertEquals("dave3", rs.getString("name"));

    assertTrue(rs.next().get());
    assertEquals(4, rs.getInt("id"));
    assertEquals("dave4", rs.getString("name"));

    assertTrue(rs.next().get());
    assertEquals(5, rs.getInt("id"));
    assertEquals("dave5", rs.getString("name"));

    rs.close();
    st.close();
  }

  @Test
  public void testInsertRowIllegalMethods() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select * from updateable").get();
    assertNotNull(rs);
    rs.moveToInsertRow();

    try {
      rs.cancelRowUpdates();
      fail("expected an exception when calling cancelRowUpdates() on the insert row");
    } catch (SQLException e) {
    }

    try {
      rs.updateRow().get();
      fail("expected an exception when calling updateRow() on the insert row");
    } catch (SQLException e) {
    }

    try {
      rs.deleteRow().get();
      fail("expected an exception when calling deleteRow() on the insert row");
    } catch (SQLException e) {
    }

    try {
      rs.refreshRow().get();
      fail("expected an exception when calling refreshRow() on the insert row");
    } catch (SQLException e) {
    }

    rs.close();
    st.close();
  }

  @Test
  public void testUpdateablePreparedStatement() throws Exception {
    // No args.
    VxPreparedStatement st = con.prepareStatement("select * from updateable",
        java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery().get();
    rs.moveToInsertRow();
    rs.close();
    st.close();

    // With args.
    st = con.prepareStatement("select * from updateable where id = ?",
        java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    st.setInt(1, 1);
    rs = st.executeQuery().get();
    rs.moveToInsertRow();
    rs.close();
    st.close();
  }

  @Test
  public void testUpdateSelectOnly() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);

    VxResultSet rs = st.executeQuery("select * from only second").get();
    assertTrue(rs.next().get());
    rs.updateInt(1, 2);
    rs.updateRow().get();
  }

  @Test
  public void testUpdateReadOnlyResultSet() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
    VxResultSet rs = st.executeQuery("select * from updateable").get();
    try {
      rs.moveToInsertRow();
      fail("expected an exception when calling moveToInsertRow() on a read-only resultset");
    } catch (SQLException e) {
    }
  }

  @Test
  public void testBadColumnIndexes() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select * from updateable").get();
    rs.moveToInsertRow();
    try {
      rs.updateInt(0, 1);
      fail("Should have thrown an exception on bad column index.");
    } catch (SQLException sqle) {
    }
    try {
      rs.updateString(1000, "hi");
      fail("Should have thrown an exception on bad column index.");
    } catch (SQLException sqle) {
    }
    try {
      rs.updateNull(1000);
      fail("Should have thrown an exception on bad column index.");
    } catch (SQLException sqle) {
    }
  }

  @Test
  public void testArray() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    stmt.executeUpdate("INSERT INTO updateable (id, intarr) VALUES (1, '{1,2,3}'::int4[])").get();
    VxResultSet rs = stmt.executeQuery("SELECT id, intarr FROM updateable").get();
    assertTrue(rs.next().get());
    rs.updateObject(2, rs.getArray(2));
    rs.updateRow().get();

    Array arr = rs.getArray(2).get();
    assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());
    rs.close();

    rs = stmt.executeQuery("SELECT id,intarr FROM updateable").get();
    assertTrue(rs.next().get());
    arr = rs.getArray(2).get();
    assertEquals(Types.INTEGER, arr.getBaseType());
    intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());

    rs.close();
    stmt.close();
  }

  @Test
  public void testMultiColumnUpdateWithoutAllColumns() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    VxResultSet rs = st.executeQuery("select id1,val from multicol").get();
    try {
      rs.moveToInsertRow();
    } catch (SQLException sqle) {
      // Ensure we're reporting that the RS is not updatable.
      assertEquals("24000", sqle.getSQLState());
    }
  }

  @Test
  public void testMultiColumnUpdate() throws Exception {
    VxStatement st =
        con.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
    st.executeUpdate("INSERT INTO multicol (id1,id2,val) VALUES (1,2,'val')").get();

    VxResultSet rs = st.executeQuery("SELECT id1, id2, val FROM multicol").get();
    assertTrue(rs.next().get());
    assertEquals("val", rs.getString("val"));
    rs.updateString("val", "newval");
    rs.updateRow().get();
    rs.close();

    rs = st.executeQuery("SELECT id1, id2, val FROM multicol").get();
    assertTrue(rs.next().get());
    assertEquals("newval", rs.getString("val"));
    rs.close();

    st.close();
  }

  @Test
  public void simpleAndUpdateableSameQuery() throws Exception {
    PGConnection unwrap = con.unwrap(PGConnection.class);
    Assume.assumeNotNull(unwrap);
    int prepareThreshold = unwrap.getPrepareThreshold();
    String sql = "select * from second where id1=?";
    for (int i = 0; i <= prepareThreshold; i++) {
      VxPreparedStatement ps = null;
      VxResultSet rs = null;
      try {
        ps = con.prepareStatement(sql);
        ps.setInt(1, 1);
        rs = ps.executeQuery().get();
        rs.next();
        String name1 = rs.getString("name1").get();
        Assert.assertEquals("anyvalue", name1);
        int id1 = rs.getInt("id1").get();
        Assert.assertEquals(1, id1);
      } finally {
        VxTestUtil.closeQuietly(rs);
        VxTestUtil.closeQuietly(ps);
      }
    }
    // The same SQL, and use updateable ResultSet
    {
      VxPreparedStatement ps = null;
      VxResultSet rs = null;
      try {
        ps = con.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_UPDATABLE);
        ps.setInt(1, 1);
        rs = ps.executeQuery().get();
        rs.next().get();
        String name1 = rs.getString("name1").get();
        Assert.assertEquals("anyvalue", name1);
        int id1 = rs.getInt("id1").get();
        Assert.assertEquals(1, id1);
        rs.updateString("name1", "updatedValue");
        rs.updateRow().get();
      } finally {
        VxTestUtil.closeQuietly(rs);
        VxTestUtil.closeQuietly(ps);
      }
    }
  }

}

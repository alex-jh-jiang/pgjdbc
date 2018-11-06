/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.postgresql.jdbc.VxCallableStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * RefCursor ResultSet tests. This test case is basically the same as the ResultSet test case.
 *
 * <p>For backwards compatibility reasons we verify that ref cursors can be
 * registered with both {@link Types#OTHER} and {@link Types#REF_CURSOR}.</p>
 *
 * @author Nic Ferrier (nferrier@tapsellferrier.co.uk)
 */
@RunWith(Parameterized.class)
public class VxRefCursorTest extends VxBaseTest4 {

  private final int cursorType;

  public VxRefCursorTest(String typeName, int cursorType) {
    this.cursorType = cursorType;
  }

  @Parameterized.Parameters(name = "typeName = {0}, cursorType = {1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"OTHER", Types.OTHER},
        //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
        {"REF_CURSOR", Types.REF_CURSOR},
        //#endif
    });
  }

  @Override
  public void setUp() throws Exception {
    // this is the same as the ResultSet setup.
    super.setUp();
    VxStatement stmt = con.createStatement();

    VxTestUtil.createTable(con, "testrs", "id integer primary key");

    stmt.executeUpdate("INSERT INTO testrs VALUES (1)").get();
    stmt.executeUpdate("INSERT INTO testrs VALUES (2)").get();
    stmt.executeUpdate("INSERT INTO testrs VALUES (3)").get();
    stmt.executeUpdate("INSERT INTO testrs VALUES (4)").get();
    stmt.executeUpdate("INSERT INTO testrs VALUES (6)").get();
    stmt.executeUpdate("INSERT INTO testrs VALUES (9)").get();


    // Create the functions.
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getRefcursor () RETURNS refcursor AS '"
        + "declare v_resset refcursor; begin open v_resset for select id from testrs order by id; "
        + "return v_resset; end;' LANGUAGE plpgsql;");
    stmt.execute("CREATE OR REPLACE FUNCTION testspg__getEmptyRefcursor () RETURNS refcursor AS '"
        + "declare v_resset refcursor; begin open v_resset for select id from testrs where id < 1 order by id; "
        + "return v_resset; end;' LANGUAGE plpgsql;");
    stmt.close();
    con.setAutoCommit(false);
  }

  @Override
  public void tearDown() throws SQLException {
    con.setAutoCommit(true);
    VxStatement stmt = con.createStatement();
    try {
      stmt.execute("drop FUNCTION testspg__getRefcursor ();").get();
      stmt.execute("drop FUNCTION testspg__getEmptyRefcursor ();").get();;
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    };
    
    try {
      VxTestUtil.dropTable(con, "testrs");
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    super.tearDown();
  }

  @Test
  public void testResult() throws SQLException, InterruptedException, ExecutionException {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall("{ ? = call testspg__getRefcursor () }");
    call.registerOutParameter(1, cursorType);
    call.execute();
    VxResultSet rs = (VxResultSet) call.getObject(1);

    assertTrue(rs.next().get());
    assertEquals(1, (Object)rs.getInt(1).get());

    assertTrue(rs.next().get());
    assertEquals(2, rs.getInt(1));

    assertTrue(rs.next().get());
    assertEquals(3, rs.getInt(1));

    assertTrue(rs.next().get());
    assertEquals(4, rs.getInt(1));

    assertTrue(rs.next().get());
    assertEquals(6, rs.getInt(1));

    assertTrue(rs.next().get());
    assertEquals(9, rs.getInt(1));

    assertFalse(rs.next().get());
    rs.close();

    call.close();
  }


  @Test
  public void testEmptyResult() throws SQLException, InterruptedException, ExecutionException {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall("{ ? = call testspg__getEmptyRefcursor () }");
    call.registerOutParameter(1, cursorType);
    call.execute();

    VxResultSet rs = (VxResultSet) call.getObject(1);
    assertTrue(!rs.next().get());
    rs.close();

    call.close();
  }

  @Test
  public void testMetaData() throws SQLException {
    assumeCallableStatementsSupported();

    VxCallableStatement call = con.prepareCall("{ ? = call testspg__getRefcursor () }");
    call.registerOutParameter(1, cursorType);
    call.execute();

    VxResultSet rs = (VxResultSet) call.getObject(1);
    ResultSetMetaData rsmd = rs.getMetaData();
    assertNotNull(rsmd);
    assertEquals(1, rsmd.getColumnCount());
    assertEquals(Types.INTEGER, rsmd.getColumnType(1));
    assertEquals("int4", rsmd.getColumnTypeName(1));
    rs.close();

    call.close();
  }

  @Test
  public void testResultType() throws SQLException {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall("{ ? = call testspg__getRefcursor () }",
        java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
    call.registerOutParameter(1, cursorType);
    call.execute();
    VxResultSet rs = (VxResultSet) call.getObject(1);

    assertEquals(rs.getType(), java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE);
    assertEquals(rs.getConcurrency(), java.sql.ResultSet.CONCUR_READ_ONLY);

    assertTrue(rs.last());
    assertEquals(6, rs.getRow());
    rs.close();
    call.close();
  }

}

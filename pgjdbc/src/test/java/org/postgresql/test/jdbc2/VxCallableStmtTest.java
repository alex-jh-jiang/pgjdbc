/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.jdbc.VxCallableStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Test;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.concurrent.ExecutionException;

/*
 * CallableStatement tests.
 *
 * @author Paul Bethe
 */
public class VxCallableStmtTest extends VxBaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    VxTestUtil.createTable(con, "int_table", "id int");
    VxStatement stmt = con.createStatement();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getString (varchar) "
        + "RETURNS varchar AS ' DECLARE inString alias for $1; begin "
        + "return ''bob''; end; ' LANGUAGE plpgsql;").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getDouble (float) "
        + "RETURNS float AS ' DECLARE inString alias for $1; begin "
        + "return 42.42; end; ' LANGUAGE plpgsql;").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getVoid (float) "
        + "RETURNS void AS ' DECLARE inString alias for $1; begin "
        + " return; end; ' LANGUAGE plpgsql;").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getInt (int) RETURNS int "
        + " AS 'DECLARE inString alias for $1; begin "
        + "return 42; end;' LANGUAGE plpgsql;").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getShort (int2) RETURNS int2 "
        + " AS 'DECLARE inString alias for $1; begin "
        + "return 42; end;' LANGUAGE plpgsql;").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getNumeric (numeric) "
        + "RETURNS numeric AS ' DECLARE inString alias for $1; "
        + "begin return 42; end; ' LANGUAGE plpgsql;").get();

    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getNumericWithoutArg() "
        + "RETURNS numeric AS '  "
        + "begin return 42; end; ' LANGUAGE plpgsql;").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__getarray() RETURNS int[] as "
        + "'SELECT ''{1,2}''::int[];' LANGUAGE sql").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__raisenotice() RETURNS int as "
        + "'BEGIN RAISE NOTICE ''hello'';  RAISE NOTICE ''goodbye''; RETURN 1; END;' LANGUAGE plpgsql").get();
    stmt.execute(
        "CREATE OR REPLACE FUNCTION testspg__insertInt(int) RETURNS int as "
        + "'BEGIN INSERT INTO int_table(id) VALUES ($1); RETURN 1; END;' LANGUAGE plpgsql").get();
    stmt.close();
  }

  @Override
  public void tearDown() throws SQLException {
    VxStatement stmt = con.createStatement();
    try {
      VxTestUtil.dropTable(con, "int_table");
    } catch (InterruptedException | ExecutionException e) {
      throw new SQLException(e);
    }
    try {
      stmt.execute("drop FUNCTION testspg__getString (varchar);").get();
      stmt.execute("drop FUNCTION testspg__getDouble (float);").get();
      stmt.execute("drop FUNCTION testspg__getVoid(float);").get();
      stmt.execute("drop FUNCTION testspg__getInt (int);").get();
      stmt.execute("drop FUNCTION testspg__getShort(int2)").get();
      stmt.execute("drop FUNCTION testspg__getNumeric (numeric);").get();

      stmt.execute("drop FUNCTION testspg__getNumericWithoutArg ();").get();
      stmt.execute("DROP FUNCTION testspg__getarray();").get();
      stmt.execute("DROP FUNCTION testspg__raisenotice();").get();
      stmt.execute("DROP FUNCTION testspg__insertInt(int);").get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SQLException(e);
    }
    
    super.tearDown();
  }


  final String func = "{ ? = call ";
  final String pkgName = "testspg__";

  @Test
  public void testGetUpdateCount() throws SQLException, InterruptedException, ExecutionException {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getDouble (?) }");
    call.setDouble(2, 3.04);
    call.registerOutParameter(1, Types.DOUBLE);
    call.execute().get();
    assertEquals(-1, call.getUpdateCount());
    assertNull(call.getResultSet());
    assertEquals(42.42, call.getDouble(1), 0.00001);
    call.close();

    // test without an out parameter
    call = con.prepareCall("{ call " + pkgName + "getDouble(?) }");
    call.setDouble(1, 3.04);
    call.execute().get();
    assertEquals(-1, call.getUpdateCount());
    VxResultSet rs = call.getResultSet();
    assertNotNull(rs);
    assertTrue(rs.next().get());
    assertEquals(42.42, rs.getDouble(1).get(), 0.00001);
    assertTrue(!rs.next().get());
    rs.close();

    assertEquals(-1, call.getUpdateCount());
    assertTrue(!call.getMoreResults());
    call.close();
  }

  @Test
  public void testGetDouble() throws Throwable {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getDouble (?) }");
    call.setDouble(2, 3.04);
    call.registerOutParameter(1, Types.DOUBLE);
    call.execute().get();
    assertEquals(42.42, call.getDouble(1), 0.00001);

    // test without an out parameter
    call = con.prepareCall("{ call " + pkgName + "getDouble(?) }");
    call.setDouble(1, 3.04);
    call.execute().get();

    call = con.prepareCall("{ call " + pkgName + "getVoid(?) }");
    call.setDouble(1, 3.04);
    call.execute().get();
  }

  @Test
  public void testGetInt() throws Throwable {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getInt (?) }");
    call.setInt(2, 4);
    call.registerOutParameter(1, Types.INTEGER);
    call.execute().get();
    assertEquals(42, call.getInt(1));
  }

  @Test
  public void testGetShort() throws Throwable {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getShort (?) }");
    call.setShort(2, (short) 4);
    call.registerOutParameter(1, Types.SMALLINT);
    call.execute().get();
    assertEquals(42, call.getShort(1));
  }

  @Test
  public void testGetNumeric() throws Throwable {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getNumeric (?) }");
    call.setBigDecimal(2, new java.math.BigDecimal(4));
    call.registerOutParameter(1, Types.NUMERIC);
    call.execute().get();
    assertEquals(new java.math.BigDecimal(42), call.getBigDecimal(1));
  }

  @Test
  public void testGetNumericWithoutArg() throws Throwable {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getNumericWithoutArg () }");
    call.registerOutParameter(1, Types.NUMERIC);
    call.execute().get();
    assertEquals(new java.math.BigDecimal(42), call.getBigDecimal(1));
  }

  @Test
  public void testGetString() throws Throwable {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getString (?) }");
    call.setString(2, "foo");
    call.registerOutParameter(1, Types.VARCHAR);
    call.execute().get();
    assertEquals("bob", call.getString(1));

  }

  @Test
  public void testGetArray() throws SQLException, InterruptedException, ExecutionException {
    assumeCallableStatementsSupported();
    VxCallableStatement call = con.prepareCall(func + pkgName + "getarray()}");
    call.registerOutParameter(1, Types.ARRAY);
    call.execute().get();
    Array arr = call.getArray(1);
    java.sql.ResultSet rs = arr.getResultSet();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(!rs.next());
  }

  @Test
  public void testRaiseNotice() throws SQLException, InterruptedException, ExecutionException {
    assumeCallableStatementsSupported();
    VxStatement statement = con.createStatement();
    statement.execute("SET SESSION client_min_messages = 'NOTICE'").get();
    VxCallableStatement call = con.prepareCall(func + pkgName + "raisenotice()}");
    call.registerOutParameter(1, Types.INTEGER);
    call.execute().get();
    SQLWarning warn = call.getWarnings();
    assertNotNull(warn);
    assertEquals("hello", warn.getMessage());
    warn = warn.getNextWarning();
    assertNotNull(warn);
    assertEquals("goodbye", warn.getMessage());
    assertEquals(1, call.getInt(1));
  }

  @Test
  public void testWasNullBeforeFetch() throws SQLException {
    VxCallableStatement cs = con.prepareCall("{? = call lower(?)}");
    cs.registerOutParameter(1, Types.VARCHAR);
    cs.setString(2, "Hi");
    try {
      cs.wasNull();
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void testFetchBeforeExecute() throws SQLException {
    VxCallableStatement cs = con.prepareCall("{? = call lower(?)}");
    cs.registerOutParameter(1, Types.VARCHAR);
    cs.setString(2, "Hi");
    try {
      cs.getString(1);
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void testFetchWithNoResults() throws SQLException {
    VxCallableStatement cs = con.prepareCall("{call now()}");
    try {
      cs.execute().get();
    } catch (InterruptedException | ExecutionException e1) {
      throw new SQLException(e1);
    }
    try {
      cs.getObject(1);
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void testBadStmt() throws Throwable {
    tryOneBadStmt("{ ?= " + pkgName + "getString (?) }");
    tryOneBadStmt("{ ?= call getString (?) ");
    tryOneBadStmt("{ = ? call getString (?); }");
  }

  protected void tryOneBadStmt(String sql) throws SQLException {
    try {
      con.prepareCall(sql);
      fail("Bad statement (" + sql + ") was not caught.");

    } catch (SQLException e) {
    }
  }

  @Test
  public void testBatchCall() throws SQLException, InterruptedException, ExecutionException {
    VxCallableStatement call = con.prepareCall("{ call " + pkgName + "insertInt(?) }");
    call.setInt(1, 1);
    call.addBatch();
    call.setInt(1, 2);
    call.addBatch();
    call.setInt(1, 3);
    call.addBatch();
    call.executeBatch().get();
    call.close();

    VxStatement stmt = con.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT id FROM int_table ORDER BY id").get();
    assertTrue(rs.next().get());
    assertEquals(1, (Object)rs.getInt(1).get());
    assertTrue(rs.next().get());
    assertEquals(2, (Object)rs.getInt(1).get());
    assertTrue(rs.next().get());
    assertEquals(3, (Object)rs.getInt(1).get());
    assertTrue(!rs.next().get());
  }

}

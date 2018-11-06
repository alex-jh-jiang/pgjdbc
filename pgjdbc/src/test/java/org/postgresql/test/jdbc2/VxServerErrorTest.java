/*
 * Copyright (c) 2013, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

/*
 * Test that enhanced error reports return the correct origin for constraint violation errors.
 */
public class VxServerErrorTest extends VxBaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeMinimumServerVersion(ServerVersion.v9_3);
    VxStatement stmt = con.createStatement();

    stmt.execute("CREATE DOMAIN testdom AS int4 CHECK (value < 10)").get();
    VxTestUtil.createTable(con, "testerr", "id int not null, val testdom not null");
    stmt.execute("ALTER TABLE testerr ADD CONSTRAINT testerr_pk PRIMARY KEY (id)").get();
    stmt.close();
  }

  @Override
  public void tearDown() throws SQLException {
    try {
      VxTestUtil.dropTable(con, "testerr");
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    VxStatement stmt = con.createStatement();
    try {
      stmt.execute("DROP DOMAIN IF EXISTS testdom").get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stmt.close();
    super.tearDown();
  }

  @Test
  public void testPrimaryKey() throws Exception {
    VxStatement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, 1)").get();
    try {
      stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, 1)").get();
      fail("Should have thrown a duplicate key exception.");
    } catch (SQLException sqle) {
      ServerErrorMessage err = ((PSQLException) sqle).getServerErrorMessage();
      assertEquals("public", err.getSchema());
      assertEquals("testerr", err.getTable());
      assertEquals("testerr_pk", err.getConstraint());
      assertNull(err.getDatatype());
      assertNull(err.getColumn());
    }
    stmt.close();
  }

  @Test
  public void testColumn() throws Exception {
    VxStatement stmt = con.createStatement();
    try {
      stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, NULL)").get();
      fail("Should have thrown a not null constraint violation.");
    } catch (SQLException sqle) {
      ServerErrorMessage err = ((PSQLException) sqle).getServerErrorMessage();
      assertEquals("public", err.getSchema());
      assertEquals("testerr", err.getTable());
      assertEquals("val", err.getColumn());
      assertNull(err.getDatatype());
      assertNull(err.getConstraint());
    }
    stmt.close();
  }

  @Test
  public void testDatatype() throws Exception {
    VxStatement stmt = con.createStatement();
    try {
      stmt.executeUpdate("INSERT INTO testerr (id, val) VALUES (1, 20)").get();
      fail("Should have thrown a constraint violation.");
    } catch (SQLException sqle) {
      ServerErrorMessage err = ((PSQLException) sqle).getServerErrorMessage();
      assertEquals("public", err.getSchema());
      assertEquals("testdom", err.getDatatype());
      assertEquals("testdom_check", err.getConstraint());
    }
    stmt.close();
  }

}

/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/*
 * Some simple tests based on problems reported by users. Hopefully these will help prevent previous
 * problems from re-occurring ;-)
 *
 */
public class VxMiscTest {

  /*
   * Some versions of the driver would return rs as a null?
   *
   * Sasha <ber0806@iperbole.bologna.it> was having this problem.
   *
   * Added Feb 13 2001
   */
  @Test
  public void testDatabaseSelectNullBug() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();

    VxStatement st = con.createStatement();
    VxResultSet rs = st.executeQuery("select datname from pg_database").get();
    assertNotNull(rs);

    while (rs.next().get()) {
      rs.getString(1);
    }

    rs.close();
    st.close();

    VxTestUtil.closeDB(con);
  }

  /**
   * Ensure the cancel call does not return before it has completed. Previously it did which
   * cancelled future queries.
   */
  @Test
  public void testSingleThreadCancel() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    VxStatement stmt = con.createStatement();
    for (int i = 0; i < 100; i++) {
      VxResultSet rs = stmt.executeQuery("SELECT 1").get();
      rs.close();
      stmt.cancel();
    }
    VxTestUtil.closeDB(con);
  }

  @Test
  public void testError() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    try {

      // transaction mode
      con.setAutoCommit(false).get();
      VxStatement stmt = con.createStatement();
      stmt.execute("select 1/0").get();
      fail("Should not execute this, as a SQLException s/b thrown");
      con.commit().get();
    } catch (SQLException ex) {
      // Verify that the SQLException is serializable.
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(ex);
      oos.close();
    }

    con.commit().get();
    con.close();
  }

  @Test
  public void testWarning() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    VxStatement stmt = con.createStatement();
    stmt.execute("CREATE TEMP TABLE t(a int primary key)").get();
    SQLWarning warning = stmt.getWarnings();
    // We should get a warning about primary key index creation
    // it's possible we won't depending on the server's
    // client_min_messages setting.
    while (warning != null) {
      // Verify that the SQLWarning is serializable.
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(warning);
      oos.close();
      warning = warning.getNextWarning();
    }

    stmt.close();
    con.close();
  }

  @Ignore
  @Test
  public void xtestLocking() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    VxConnection con2 = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con, "test_lock", "name text");
    VxStatement st = con.createStatement();
    VxStatement st2 = con2.createStatement();
    con.setAutoCommit(false).get();
    st.execute("lock table test_lock").get();
    st2.executeUpdate("insert into test_lock ( name ) values ('hello')").get();
    con.commit().get();
    VxTestUtil.dropTable(con, "test_lock");
    con.close();
    con2.close();
  }
}

/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/*
 * This test suite will check the behaviour of the findColumnIndex method. This is testing the
 * behaviour when sanitiser is disabled.
 */
public class VxColumnSanitiserDisabledTest {
  private VxConnection conn;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("disableColumnSanitiser", Boolean.TRUE.toString());
    conn = VxTestUtil.openDB(props).get();
    assertTrue(conn instanceof BaseConnection);
    BaseConnection bc = (BaseConnection) conn;
    assertTrue("Expected state [TRUE] of base connection configuration failed test.",
        bc.isColumnSanitiserDisabled());
    /*
     * Quoted columns will be stored with case preserved. Driver will receive column names as
     * defined in db server.
     */
    VxTestUtil.createTable(conn, "allmixedup",
        "id int primary key, \"DESCRIPTION\" varchar(40), \"fOo\" varchar(3)");
    VxStatement data = conn.createStatement();
    data.execute(VxTestUtil.insertSQL("allmixedup", "1,'mixed case test', 'bar'")).get();
    data.close();
  }

  @After
  public void tearDown() throws Exception {
    VxTestUtil.dropTable(conn, "allmixedup");
    VxTestUtil.closeDB(conn);
    System.setProperty("disableColumnSanitiser", "false");
  }

  /*
   * Test cases checking different combinations of columns origination from database against
   * application supplied column names.
   */

  @Test
  public void testTableColumnLowerNowFindFindLowerCaseColumn() throws SQLException {
    findColumn("id", true);
  }

  @Test
  public void testTableColumnLowerNowFindFindUpperCaseColumn() throws SQLException {
    findColumn("ID", true);
  }

  @Test
  public void testTableColumnLowerNowFindFindMixedCaseColumn() throws SQLException {
    findColumn("Id", false);
  }

  @Test
  public void testTableColumnUpperNowFindFindLowerCaseColumn() throws SQLException {
    findColumn("description", true);
  }

  @Test
  public void testTableColumnUpperNowFindFindUpperCaseColumn() throws SQLException {
    findColumn("DESCRIPTION", true);
  }

  @Test
  public void testTableColumnUpperNowFindFindMixedCaseColumn() throws SQLException {
    findColumn("Description", false);
  }

  @Test
  public void testTableColumnMixedNowFindLowerCaseColumn() throws SQLException {
    findColumn("foo", false);
  }

  @Test
  public void testTableColumnMixedNowFindFindUpperCaseColumn() throws SQLException {
    findColumn("FOO", false);
  }

  @Test
  public void testTableColumnMixedNowFindFindMixedCaseColumn() throws SQLException {
    findColumn("fOo", true);
  }

  private void findColumn(String label, boolean failOnNotFound) throws SQLException {
    VxPreparedStatement query = conn.prepareStatement("select * from allmixedup");
    try {
      if ((VxTestUtil.findColumn(query, label).get() == 0) && failOnNotFound) {
        fail(String.format("Expected to find the column with the label [%1$s].", label));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new SQLException(e);
    }
    query.close();
  }
}

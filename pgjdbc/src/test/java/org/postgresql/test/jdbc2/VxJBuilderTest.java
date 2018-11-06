/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertNotNull;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/*
 * Some simple tests to check that the required components needed for JBuilder stay working
 *
 */
public class VxJBuilderTest {

  // Set up the fixture for this testcase: the tables for this test.
  @Before
  public void setUp() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con, "test_c", "source text,cost money,imageid int4");

    VxTestUtil.closeDB(con);
  }

  // Tear down the fixture for this test case.
  @After
  public void tearDown() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    VxTestUtil.dropTable(con, "test_c");
    VxTestUtil.closeDB(con);
  }

  /*
   * This tests that Money types work. JDBCExplorer barfs if this fails.
   */
  @Test
  public void testMoney() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();

    VxStatement st = con.createStatement();
    VxResultSet rs = st.executeQuery("select cost from test_c").get();
    assertNotNull(rs);

    while (rs.next().get()) {
      rs.getDouble(1);
    }

    rs.close();
    st.close();

    VxTestUtil.closeDB(con);
  }
}

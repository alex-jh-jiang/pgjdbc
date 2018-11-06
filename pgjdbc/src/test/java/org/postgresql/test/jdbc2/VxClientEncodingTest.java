/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.postgresql.PGProperty;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.PSQLState;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RunWith(Parameterized.class)
public class VxClientEncodingTest extends VxBaseTest4 {

  @Parameterized.Parameter(0)
  public boolean allowEncodingChanges;

  @Override
  protected void updateProperties(Properties props) {
    super.updateProperties(props);
    PGProperty.ALLOW_ENCODING_CHANGES.set(props, allowEncodingChanges);
  }

  @Parameterized.Parameters(name = "allowEncodingChanges={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {true},
        {false}
    });
  }

  @Test
  public void setEncodingUtf8() throws SQLException, InterruptedException, ExecutionException {
    // UTF-8 is a default encoding, so it should always be safe to set encoding to UTF-8
    setEncoding("UTF-8");

    checkConnectionSanity();
  }

  @Test
  public void setEncodingAscii() throws SQLException, InterruptedException, ExecutionException {
    try {
      setEncoding("sql_ascii");
      if (!allowEncodingChanges) {
        Assert.fail(
            "allowEncodingChanges is false, thus set client_encoding=aql_ascii is expected to fail");
      }
    } catch (SQLException e) {
      if (!allowEncodingChanges && !PSQLState.CONNECTION_FAILURE.getState()
          .equals(e.getSQLState())) {
        throw e;
      }
      Assert.assertTrue("Connection should be closed on client_encoding change", con.isClosed());
      return;
    }

    checkConnectionSanity();
  }

  private void checkConnectionSanity() throws SQLException, InterruptedException, ExecutionException {
    VxStatement st = con.createStatement();
    VxResultSet rs = st.executeQuery("select 'abc' as x").get();
    rs.next();
    Assert.assertEquals("abc", rs.getString(1));
    VxTestUtil.closeQuietly(rs);
    VxTestUtil.closeQuietly(st);
  }

  private void setEncoding(String encoding) throws SQLException, InterruptedException, ExecutionException {
    VxStatement st = con.createStatement();
    st.execute("set client_encoding='" + encoding + "'").get();
    VxTestUtil.closeQuietly(st);
  }
}

/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.test.VxTestUtil;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class VxConcurrentStatementFetch extends VxBaseTest4 {

  private final AutoCommit autoCommit;
  private final int fetchSize;

  public VxConcurrentStatementFetch(AutoCommit autoCommit, int fetchSize, BinaryMode binaryMode) {
    this.autoCommit = autoCommit;
    this.fetchSize = fetchSize;
    setBinaryMode(binaryMode);
  }

  @Parameterized.Parameters(name = "{index}: fetch(autoCommit={0}, fetchSize={1}, binaryMode={2})")
  public static Iterable<Object[]> data() {
    Collection<Object[]> ids = new ArrayList<Object[]>();
    for (AutoCommit autoCommit : AutoCommit.values()) {
      for (int fetchSize : new int[]{1, 2, 20}) {
        for (BinaryMode binaryMode : BinaryMode.values()) {
          ids.add(new Object[]{autoCommit, fetchSize, binaryMode});
        }
      }
    }
    return ids;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    con.setAutoCommit(autoCommit == AutoCommit.YES);
  }

  @Test
  public void testFetchTwoStatements() throws Exception {
    // This test definitely fails at 8.2 in autocommit=false, and works with 8.4+
    Assume.assumeTrue(autoCommit == AutoCommit.YES
        || VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4));
    VxPreparedStatement ps1 = null;
    VxPreparedStatement ps2 = null;
    try {
      ps1 = con.prepareStatement("select * from generate_series(0, 9)");
      ps1.setFetchSize(fetchSize);
      VxResultSet rs1 = ps1.executeQuery().get();
      ps2 = con.prepareStatement("select * from generate_series(10, 19)");
      ps2.setFetchSize(fetchSize);
      VxResultSet rs2 = ps2.executeQuery().get();

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(rs1.next().get());
        Assert.assertTrue(rs2.next().get());
        Assert.assertEquals("Row#" + i + ", resultset 1", i, rs1.getInt(1));
        Assert.assertEquals("Row#" + i + ", resultset 2", i + 10, rs2.getInt(1));
      }
      Assert.assertFalse(rs1.next().get());
      Assert.assertFalse(rs2.next().get());
    } finally {
      VxTestUtil.closeQuietly(ps1);
      VxTestUtil.closeQuietly(ps2);
    }
  }
}

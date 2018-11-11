/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class VxTypeCacheDLLStressTest extends VxBaseTest4 {
  private static final int DURATION = Integer.getInteger("TypeCacheDLLStressTest.DURATION", 5);

  private VxConnection con2;

  @Override
  protected void updateProperties(Properties props) {
    try {
      con2 = VxTestUtil.openDB(props).get();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to open second DB connection", e);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    VxTestUtil.createTable(con, "create_and_drop_table", "user_id serial PRIMARY KEY");
  }

  @Override
  public void tearDown() throws SQLException {
    VxTestUtil.closeDB(con2);
  }

  @Test
  public void createDropTableAndGetTypeInfo() throws Throwable {
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Future<Void> typeInfoCache = executor.submit(new Callable<Void>() {
      public Void call() throws Exception {
        while (!Thread.currentThread().isInterrupted()) {
          VxResultSet rs = con.getMetaData().getTypeInfo().get();
          rs.close();
        }
        return null;
      }
    });

    Future<Void> createAndDrop = executor.submit(new Callable<Void>() {
      public Void call() throws Exception {
        VxStatement stmt = con2.createStatement();

        while (!Thread.currentThread().isInterrupted()) {
          stmt.execute("drop TABLE create_and_drop_table").get();
          stmt.execute("CREATE TABLE create_and_drop_table"
              + "( user_id serial PRIMARY KEY, username VARCHAR (50) UNIQUE NOT NULL"
              + ", password VARCHAR (50) NOT NULL, email VARCHAR (355) UNIQUE NOT NULL"
              + ", created_on TIMESTAMP NOT NULL, last_login TIMESTAMP)").get();
        }
        return null;
      }
    });

    try {
      typeInfoCache.get(DURATION, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      createAndDrop.cancel(true);
      throw e.getCause();
    } catch (TimeoutException e) {
      // Test is expected to run as long as it can
    }

    typeInfoCache.cancel(true);
    createAndDrop.cancel(true);

    try {
      createAndDrop.get(DURATION, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    } catch (TimeoutException e) {
      // Test is expected to run as long as it can
    } catch (CancellationException e) {
      // Ignore
    }
  }
}

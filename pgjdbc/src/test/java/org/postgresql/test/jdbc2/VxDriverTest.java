/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.Driver;
import org.postgresql.PGProperty;
import org.postgresql.VxDriver;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.NullOutputStream;
import org.postgresql.util.WriterHandler;

import org.junit.Test;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Logger;

/*
 * Tests the dynamically created class org.postgresql.Driver
 *
 */
public class VxDriverTest {

  @Test
  public void urlIsNotForPostgreSQL() throws SQLException {
    Driver driver = new Driver();

    assertNull(driver.connect("jdbc:otherdb:database", new Properties()));
  }

  /*
   * This tests the acceptsURL() method with a couple of well and poorly formed jdbc urls.
   */
  @Test
  public void testAcceptsURL() throws Exception {
    VxTestUtil.initDriver(); // Set up log levels, etc.

    // Load the driver (note clients should never do it this way!)
    org.postgresql.Driver drv = new org.postgresql.Driver();
    assertNotNull(drv);

    // These are always correct
    verifyUrl(drv, "jdbc:postgresql:test", "localhost", "5432", "test");
    verifyUrl(drv, "jdbc:postgresql://localhost/test", "localhost", "5432", "test");
    verifyUrl(drv, "jdbc:postgresql://localhost:5432/test", "localhost", "5432", "test");
    verifyUrl(drv, "jdbc:postgresql://127.0.0.1/anydbname", "127.0.0.1", "5432", "anydbname");
    verifyUrl(drv, "jdbc:postgresql://127.0.0.1:5433/hidden", "127.0.0.1", "5433", "hidden");
    verifyUrl(drv, "jdbc:postgresql://[::1]:5740/db", "[::1]", "5740", "db");

    // Badly formatted url's
    assertTrue(!drv.acceptsURL("jdbc:postgres:test"));
    assertTrue(!drv.acceptsURL("postgresql:test"));
    assertTrue(!drv.acceptsURL("db"));
    assertTrue(!drv.acceptsURL("jdbc:postgresql://localhost:5432a/test"));

    // failover urls
    verifyUrl(drv, "jdbc:postgresql://localhost,127.0.0.1:5432/test", "localhost,127.0.0.1",
        "5432,5432", "test");
    verifyUrl(drv, "jdbc:postgresql://localhost:5433,127.0.0.1:5432/test", "localhost,127.0.0.1",
        "5433,5432", "test");
    verifyUrl(drv, "jdbc:postgresql://[::1],[::1]:5432/db", "[::1],[::1]", "5432,5432", "db");
    verifyUrl(drv, "jdbc:postgresql://[::1]:5740,127.0.0.1:5432/db", "[::1],127.0.0.1", "5740,5432",
        "db");
  }

  private void verifyUrl(Driver drv, String url, String hosts, String ports, String dbName)
      throws Exception {
    assertTrue(url, drv.acceptsURL(url));
    Method parseMethod =
        drv.getClass().getDeclaredMethod("parseURL", String.class, Properties.class);
    parseMethod.setAccessible(true);
    Properties p = (Properties) parseMethod.invoke(drv, url, null);
    assertEquals(url, dbName, p.getProperty(PGProperty.PG_DBNAME.getName()));
    assertEquals(url, hosts, p.getProperty(PGProperty.PG_HOST.getName()));
    assertEquals(url, ports, p.getProperty(PGProperty.PG_PORT.getName()));
  }

  /**
   * Tests the connect method by connecting to the test database
   */
  @Test
  public void testConnect() throws Exception {
    VxTestUtil.initDriver(); // Set up log levels, etc.

    // Test with the url, username & password
    Properties info = new Properties();
    VxConnection con = 
        new VxDriver().connect(VxTestUtil.getURL(), info).get();
    assertNotNull(con);
    con.close();

    // disable by alex.jiang 20181104
    // Test with the username in the url
    //con = DriverManager.getConnection(
      //  VxTestUtil.getURL() + "&user=" + VxTestUtil.getUser() + "&password=" + VxTestUtil.getPassword());
    //assertNotNull(con);
    //con.close();

    // Test with failover url
  }

  /**
   * Tests that pgjdbc performs connection failover if unable to connect to the first host in the
   * URL.
   *
   * @throws Exception if something wrong happens
   */
//  @Test
  public void testConnectFailover() throws Exception {
    String url = "jdbc:postgresql://invalidhost.not.here:5432," + VxTestUtil.getServer() + ":"
        + VxTestUtil.getPort() + "/" + VxTestUtil.getDatabase() + "?connectTimeout=5";
    Properties info = new Properties();
    VxConnection con = 
        new VxDriver().connect(VxTestUtil.getURL(), info).get();
    assertNotNull(con);
    con.close();
  }

  /*
   * Test that the readOnly property works.
   */
  @Test
  public void testReadOnly() throws Exception {
    VxTestUtil.initDriver(); // Set up log levels, etc.
    Properties info = new Properties();
    VxConnection con = 
        new VxDriver().connect(VxTestUtil.getURL() + "&readOnly=true", info).get();
    assertNotNull(con);
    assertTrue(con.isReadOnly());
    con.close();

    con = 
        new VxDriver().connect(VxTestUtil.getURL() + "&readOnly=true", info).get();
    assertNotNull(con);
    assertFalse(con.isReadOnly());
    con.close();

    con = 
        new VxDriver().connect(VxTestUtil.getURL(), info).get();
    assertNotNull(con);
    assertFalse(con.isReadOnly());
    con.close();
  }

  public void testRegistration() throws Exception {
  }

  @Deprecated
  public void testSetLogWriter() throws Exception {
/*
    // this is a dummy to make sure VxTestUtil is initialized
    Properties info = new Properties();
    VxConnection con = 
        new VxDriver().connect(VxTestUtil.getURL(), info).get();
    con.close();
    String loggerLevel = System.getProperty("loggerLevel");
    String loggerFile = System.getProperty("loggerFile");

    try {

      PrintWriter printWriter = new PrintWriter(new NullOutputStream(System.err));
      DriverManager.setLogWriter(printWriter);
      assertEquals(DriverManager.getLogWriter(), printWriter);
      System.clearProperty("loggerFile");
      System.clearProperty("loggerLevel");
      Properties props = new Properties();
      props.setProperty("user", VxTestUtil.getUser());
      props.setProperty("password", VxTestUtil.getPassword());
      props.setProperty("loggerLevel", "DEBUG");
      con = DriverManager.getConnection(VxTestUtil.getURL(), props);

      Logger logger = Logger.getLogger("org.postgresql");
      Handler[] handlers = logger.getHandlers();
      assertTrue(handlers[0] instanceof WriterHandler );
      con.close();
    } finally {
      DriverManager.setLogWriter(null);
      System.setProperty("loggerLevel", loggerLevel);
      System.setProperty("loggerFile", loggerFile);

    }
*/
  }

  @Test
  public void testSetLogStream() throws Exception {
/*
    // this is a dummy to make sure VxTestUtil is initialized
    Connection con = DriverManager.getConnection(VxTestUtil.getURL(), VxTestUtil.getUser(), VxTestUtil.getPassword());
    con.close();
    String loggerLevel = System.getProperty("loggerLevel");
    String loggerFile = System.getProperty("loggerFile");

    try {

      DriverManager.setLogStream(new NullOutputStream(System.err));
      System.clearProperty("loggerFile");
      System.clearProperty("loggerLevel");
      Properties props = new Properties();
      props.setProperty("user", VxTestUtil.getUser());
      props.setProperty("password", VxTestUtil.getPassword());
      props.setProperty("loggerLevel", "DEBUG");
      con = DriverManager.getConnection(VxTestUtil.getURL(), props);

      Logger logger = Logger.getLogger("org.postgresql");
      Handler []handlers = logger.getHandlers();
      assertTrue( handlers[0] instanceof WriterHandler );
      con.close();
    } finally {
      DriverManager.setLogStream(null);
      System.setProperty("loggerLevel", loggerLevel);
      System.setProperty("loggerFile", loggerFile);


    }
*/
  }

}

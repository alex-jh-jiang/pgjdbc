/*
 * Copyright (c) 2005, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.VxDriver;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.test.VxTestUtil;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Properties;

public class VxLoginTimeoutTest {

  @Before
  public void setUp() throws Exception {
    VxTestUtil.initDriver(); // Set up log levels, etc.
  }

  @Test
  public void testIntTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", VxTestUtil.getUser());
    props.setProperty("password", VxTestUtil.getPassword());
    props.setProperty("loginTimeout", "10");

    VxConnection conn = new VxDriver().connect(VxTestUtil.getURL(), props).get();
    conn.close();
  }

  @Test
  public void testFloatTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", VxTestUtil.getUser());
    props.setProperty("password", VxTestUtil.getPassword());
    props.setProperty("loginTimeout", "10.0");

    VxConnection conn = new VxDriver().connect(VxTestUtil.getURL(), props).get();
    conn.close();
  }

  @Test
  public void testZeroTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", VxTestUtil.getUser());
    props.setProperty("password", VxTestUtil.getPassword());
    props.setProperty("loginTimeout", "0");

    VxConnection conn = new VxDriver().connect(VxTestUtil.getURL(), props).get();
    conn.close();
  }

  @Test
  public void testNegativeTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", VxTestUtil.getUser());
    props.setProperty("password", VxTestUtil.getPassword());
    props.setProperty("loginTimeout", "-1");

    VxConnection conn = new VxDriver().connect(VxTestUtil.getURL(), props).get();
    conn.close();
  }

  @Test
  public void testBadTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", VxTestUtil.getUser());
    props.setProperty("password", VxTestUtil.getPassword());
    props.setProperty("loginTimeout", "zzzz");

    VxConnection conn = new VxDriver().connect(VxTestUtil.getURL(), props).get();
    conn.close();
  }

  private static class TimeoutHelper implements Runnable {
    TimeoutHelper() throws IOException {
      InetAddress localAddr;
      try {
        localAddr = InetAddress.getLocalHost();
      } catch (UnknownHostException ex) {
        System.err.println("WARNING: Could not resolve local host name, trying 'localhost'. " + ex);
        localAddr = InetAddress.getByName("localhost");
      }
      this.listenSocket = new ServerSocket(0, 1, localAddr);
    }

    String getHost() {
      return listenSocket.getInetAddress().getHostAddress();
    }

    int getPort() {
      return listenSocket.getLocalPort();
    }

    @Override
    public void run() {
      try {
        Socket newSocket = listenSocket.accept();
        try {
          Thread.sleep(30000);
        } catch (InterruptedException e) {
          // Ignore it.
        }
        newSocket.close();
      } catch (IOException e) {
        // Ignore it.
      }
    }

    void kill() {
      try {
        listenSocket.close();
      } catch (IOException e) {
      }
    }

    private final ServerSocket listenSocket;
  }


  @Test
  public void testTimeoutOccurs() throws Exception {
    // Spawn a helper thread to accept a connection and do nothing with it;
    // this should trigger a timeout.
    TimeoutHelper helper = new TimeoutHelper();
    new Thread(helper, "timeout listen helper").start();

    try {
      String url = "jdbc:postgresql://" + helper.getHost() + ":" + helper.getPort() + "/dummy";
      Properties props = new Properties();
      props.setProperty("user", "dummy");
      props.setProperty("loginTimeout", "5");

      // This is a pretty crude check, but should help distinguish
      // "can't connect" from "timed out".
      long startTime = System.currentTimeMillis();
      VxConnection conn = null;
      try {
        conn = new VxDriver().connect(url, props).get();
        fail("connection was unexpectedly successful");
      } catch (SQLException e) {
        // Ignored.
      } finally {
        if (conn != null) {
          conn.close();
        }
      }

      long endTime = System.currentTimeMillis();
      assertTrue(endTime > startTime + 2500);
    } finally {
      helper.kill();
    }
  }
}


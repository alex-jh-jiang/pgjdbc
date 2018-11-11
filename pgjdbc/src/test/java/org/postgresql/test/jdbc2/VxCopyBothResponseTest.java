/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.copy.CopyDual;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.test.VxTestUtil;
import org.postgresql.test.util.rules.ServerVersionRule;
import org.postgresql.test.util.rules.annotation.HaveMinimalServerVersion;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * CopyBothResponse use since 9.1 PostgreSQL version for replication protocol
 */
@HaveMinimalServerVersion("9.4")
public class VxCopyBothResponseTest {
  @Rule
  public ServerVersionRule versionRule = new ServerVersionRule();

  private VxConnection sqlConnection;
  private VxConnection replConnection;

  @BeforeClass
  public static void beforeClass() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con, "testreplication", "pk serial primary key, name varchar(100)");
    con.close();
  }

  @AfterClass
  public static void testAfterClass() throws Exception {
    VxConnection con = VxTestUtil.openDB().get();
    VxTestUtil.dropTable(con, "testreplication");
    con.close();
  }

  @Before
  public void setUp() throws Exception {
    sqlConnection = VxTestUtil.openDB().get();
    replConnection = openReplicationConnection();
    replConnection.setAutoCommit(true);
  }

  @After
  public void tearDown() throws Exception {
    sqlConnection.close();
    replConnection.close();
  }

  @Test
  public void testOpenConnectByReplicationProtocol() throws Exception {
    CopyManager cm = ((PGConnection) replConnection).getCopyAPI();

    LogSequenceNumber logSequenceNumber = getCurrentLSN();
    CopyDual copyDual = cm.copyDual(
        "START_REPLICATION " + logSequenceNumber.asString()).get();
    try {
      assertThat(
          "Replication protocol work via copy protocol and initialize as CopyBothResponse, "
              + "we want that first initialize will work",
          copyDual, CoreMatchers.notNullValue()
      );
    } finally {
      copyDual.endCopy().get();
    }
  }

  @Test
  public void testReceiveKeepAliveMessage() throws Exception {
    CopyManager cm = ((PGConnection) replConnection).getCopyAPI();

    LogSequenceNumber logSequenceNumber = getCurrentLSN();
    CopyDual copyDual = cm.copyDual(
        "START_REPLICATION " + logSequenceNumber.asString()).get();

    sendStandByUpdate(copyDual, logSequenceNumber, logSequenceNumber, logSequenceNumber, true);
    ByteBuffer buf = ByteBuffer.wrap(copyDual.readFromCopy().get());

    int code = buf.get();
    copyDual.endCopy().get();

    assertThat(
        "Streaming replication start with swap keep alive message, we want that first get packege will be keep alive",
        code, equalTo((int) 'k')
    );
  }

  @Test
  public void testKeedAliveContaintCorrectLSN() throws Exception {
    CopyManager cm = ((PGConnection) replConnection).getCopyAPI();

    LogSequenceNumber startLsn = getCurrentLSN();
    CopyDual copyDual =
        cm.copyDual("START_REPLICATION " + startLsn.asString()).get();
    sendStandByUpdate(copyDual, startLsn, startLsn, startLsn, true);

    ByteBuffer buf = ByteBuffer.wrap(copyDual.readFromCopy().get());

    int code = buf.get();
    LogSequenceNumber lastLSN = LogSequenceNumber.valueOf(buf.getLong());
    copyDual.endCopy().get();

    assertThat(
        "Keep alive message contain last lsn on server, we want that before start replication "
            + "and get keep alive message not occurs wal modifications",
        lastLSN, CoreMatchers.equalTo(startLsn)
    );
  }

  @Test
  public void testReceiveXLogData() throws Exception {
    CopyManager cm = ((PGConnection) replConnection).getCopyAPI();

    LogSequenceNumber startLsn = getCurrentLSN();

    VxStatement st = sqlConnection.createStatement();
    st.execute("insert into testreplication(name) values('testing get changes')").get();
    st.close();

    CopyDual copyDual =
        cm.copyDual("START_REPLICATION " + startLsn.asString()).get();
    sendStandByUpdate(copyDual, startLsn, startLsn, startLsn, false);

    ByteBuffer buf = ByteBuffer.wrap(copyDual.readFromCopy().get());

    char code = (char) buf.get();
    copyDual.endCopy().get();

    assertThat(
        "When replication starts via slot and specify LSN that lower than last LSN on server, "
            + "we should get all changes that occurs beetween two LSN",
        code, equalTo('w')
    );
  }

  private void sendStandByUpdate(CopyDual copyDual, LogSequenceNumber received,
      LogSequenceNumber flushed, LogSequenceNumber applied, boolean replyRequired)
      throws SQLException {
    ByteBuffer response = ByteBuffer.allocate(1 + 8 + 8 + 8 + 8 + 1);
    response.put((byte) 'r');
    response.putLong(received.asLong()); //received
    response.putLong(flushed.asLong()); //flushed
    response.putLong(applied.asLong()); //applied
    response.putLong(TimeUnit.MICROSECONDS.convert((System.currentTimeMillis() - 946674000000L),
        TimeUnit.MICROSECONDS));
    response.put(replyRequired ? (byte) 1 : (byte) 0); //reply soon as possible

    byte[] standbyUpdate = response.array();
    try {
		copyDual.writeToCopy(standbyUpdate, 0, standbyUpdate.length).get();
	} catch (InterruptedException | ExecutionException e1) {
		throw new SQLException(e1);
	}
    try {
		copyDual.flushCopy().get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
  }

  private LogSequenceNumber getCurrentLSN() throws SQLException, InterruptedException, ExecutionException {
    VxStatement st = sqlConnection.createStatement();
    VxResultSet rs = null;
    try {
      rs = st.executeQuery("select "
          + (((BaseConnection) sqlConnection).haveMinimumServerVersion(ServerVersion.v10)
          ? "pg_current_wal_lsn()" : "pg_current_xlog_location()")).get();

      if (rs.next().get()) {
        String lsn = rs.getString(1).get();
        return LogSequenceNumber.valueOf(lsn);
      } else {
        return LogSequenceNumber.INVALID_LSN;
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
      st.close();
    }
  }

  private VxConnection openReplicationConnection() throws Exception {
    Properties properties = new Properties();
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
    PGProperty.PROTOCOL_VERSION.set(properties, "3");
    PGProperty.REPLICATION.set(properties, "database");
    return VxTestUtil.openDB(properties).get();
  }
}

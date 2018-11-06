/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.VxCallableStatement;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.test.util.BufferGenerator;
import org.postgresql.test.util.StrangeInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * @author amozhenin on 30.09.2015.
 */
public class VxCopyLargeFileTest {

  private static final int FEED_COUNT = 10;

  private VxConnection con;
  private CopyManager copyAPI;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con, "pgjdbc_issue366_test_glossary",
        "id SERIAL, text_id VARCHAR(1000) NOT NULL UNIQUE, name VARCHAR(10) NOT NULL UNIQUE");
    VxTestUtil.createTable(con, "pgjdbc_issue366_test_data",
        "id SERIAL,\n"
            + "data_text_id VARCHAR(1000) NOT NULL /*UNIQUE <-- it slows down inserts due to additional index */,\n"
            + "glossary_text_id VARCHAR(1000) NOT NULL /* REFERENCES pgjdbc_issue366_test_glossary(text_id) */,\n"
            + "value DOUBLE PRECISION NOT NULL");

    feedTable();
    BufferGenerator.main(new String[]{});
    copyAPI = ((PGConnection) con).getCopyAPI();
  }

  private void feedTable() throws Exception {
    VxPreparedStatement stmt = con.prepareStatement(
        VxTestUtil.insertSQL("pgjdbc_issue366_test_glossary", "text_id, name", "?, ?"));
    for (int i = 0; i < 26; i++) {
      char ch = (char) ('A' + i); // black magic
      insertData(stmt, "VERY_LONG_STRING_TO_REPRODUCE_ISSUE_366_" + ch + ch + ch,
          "" + ch + ch + ch);
    }
  }

  private void insertData(VxPreparedStatement stmt, String textId, String name) throws SQLException {
    stmt.setString(1, textId);
    stmt.setString(2, name);
    stmt.executeUpdate();
  }

  @After
  public void tearDown() throws Exception {
    try {
      VxTestUtil.dropTable(con, "pgjdbc_issue366_test_data");
      VxTestUtil.dropTable(con, "pgjdbc_issue366_test_glossary");
      new File("target/buffer.txt").delete();
    } finally {
      con.close();
    }
  }

  @Test
  public void testFeedTableSeveralTimesTest() throws Exception {
    for (int i = 1; i <= FEED_COUNT; i++) {
      feedTableAndCheckTableFeedIsOk(con);
      cleanupTable(con);
    }
  }

  private void feedTableAndCheckTableFeedIsOk(VxConnection conn) throws Exception {
    InputStream in = null;
    try {
      in = new StrangeInputStream(new FileInputStream("target/buffer.txt"));
      long size = copyAPI.copyIn(
          "COPY pgjdbc_issue366_test_data(data_text_id, glossary_text_id, value) FROM STDIN", in).get();
      assertEquals(BufferGenerator.ROW_COUNT, size);
    } finally {
      if (in != null) {
        in.close();
      }
    }

  }

  private void cleanupTable(VxConnection conn) throws Exception {
    VxCallableStatement stmt = null;
    try {
      stmt = conn.prepareCall("TRUNCATE pgjdbc_issue366_test_data;");
      stmt.execute();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }

  }
}

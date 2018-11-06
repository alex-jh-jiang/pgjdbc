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

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxDatabaseMetaData;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/*
 * TestCase to test the internal functionality of org.postgresql.jdbc2.DatabaseMetaData
 *
 */
public class VxDatabaseMetaDataTest {
  private VxConnection con;

  @Before
  public void setUp() throws Exception {
    con = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con, "metadatatest",
        "id int4, name text, updated timestamptz, colour text, quest text");
    VxTestUtil.dropSequence(con, "sercoltest_b_seq");
    VxTestUtil.dropSequence(con, "sercoltest_c_seq");
    VxTestUtil.createTable(con, "sercoltest", "a int, b serial, c bigserial");
    VxTestUtil.createTable(con, "\"a\\\"", "a int4");
    VxTestUtil.createTable(con, "\"a'\"", "a int4");
    VxTestUtil.createTable(con, "arraytable", "a numeric(5,2)[], b varchar(100)[]");
    VxTestUtil.createTable(con, "intarraytable", "a int4[], b int4[][]");
    VxTestUtil.createCompositeType(con, "custom", "i int");
    VxTestUtil.createCompositeType(con, "_custom", "f float");


    // 8.2 does not support arrays of composite types
    VxTestUtil.createTable(con, "customtable", "c1 custom, c2 _custom"
        + (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_3) ? ", c3 custom[], c4 _custom[]" : ""));

    VxStatement stmt = con.createStatement();
    // we add the following comments to ensure the joins to the comments
    // are done correctly. This ensures we correctly test that case.
    stmt.execute("comment on table metadatatest is 'this is a table comment'");
    stmt.execute("comment on column metadatatest.id is 'this is a column comment'");

    stmt.execute(
        "CREATE OR REPLACE FUNCTION f1(int, varchar) RETURNS int AS 'SELECT 1;' LANGUAGE SQL");
    stmt.execute(
        "CREATE OR REPLACE FUNCTION f2(a int, b varchar) RETURNS int AS 'SELECT 1;' LANGUAGE SQL");
    stmt.execute(
        "CREATE OR REPLACE FUNCTION f3(IN a int, INOUT b varchar, OUT c timestamptz) AS $f$ BEGIN b := 'a'; c := now(); return; END; $f$ LANGUAGE plpgsql");
    stmt.execute(
        "CREATE OR REPLACE FUNCTION f4(int) RETURNS metadatatest AS 'SELECT 1, ''a''::text, now(), ''c''::text, ''q''::text' LANGUAGE SQL");
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4)) {
      // RETURNS TABLE requires PostgreSQL 8.4+
      stmt.execute(
          "CREATE OR REPLACE FUNCTION f5() RETURNS TABLE (i int) LANGUAGE sql AS 'SELECT 1'");
    }

    VxTestUtil.createDomain(con, "nndom", "int not null");
    VxTestUtil.createTable(con, "domaintable", "id nndom");
    stmt.close();
  }

  @After
  public void tearDown() throws Exception {
    // Drop function first because it depends on the
    // metadatatest table's type
    VxStatement stmt = con.createStatement();
    stmt.execute("DROP FUNCTION f4(int)");

    VxTestUtil.dropTable(con, "metadatatest");
    VxTestUtil.dropTable(con, "sercoltest");
    VxTestUtil.dropSequence(con, "sercoltest_b_seq");
    VxTestUtil.dropSequence(con, "sercoltest_c_seq");
    VxTestUtil.dropTable(con, "\"a\\\"");
    VxTestUtil.dropTable(con, "\"a'\"");
    VxTestUtil.dropTable(con, "arraytable");
    VxTestUtil.dropTable(con, "intarraytable");
    VxTestUtil.dropTable(con, "customtable");
    VxTestUtil.dropType(con, "custom");
    VxTestUtil.dropType(con, "_custom");

    stmt.execute("DROP FUNCTION f1(int, varchar)");
    stmt.execute("DROP FUNCTION f2(int, varchar)");
    stmt.execute("DROP FUNCTION f3(int, varchar)");
    VxTestUtil.dropType(con, "domaintable");
    VxTestUtil.dropDomain(con, "nndom");

    VxTestUtil.closeDB(con);
  }

  @Test
  public void testArrayTypeInfo() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getColumns(null, null, "intarraytable", "a");
    assertTrue(rs.next().get());
    assertEquals("_int4", rs.getString("TYPE_NAME"));
    con.createArrayOf("integer", new Integer[] {});
    VxTestUtil.closeQuietly(rs);
    rs = dbmd.getColumns(null, null, "intarraytable", "a");
    assertTrue(rs.next().get());
    assertEquals("_int4", rs.getString("TYPE_NAME"));
    VxTestUtil.closeQuietly(rs);
  }

  @Test
  public void testArrayInt4DoubleDim() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getColumns(null, null, "intarraytable", "b");
    assertTrue(rs.next().get());
    assertEquals("_int4", rs.getString("TYPE_NAME")); // even int4[][] is represented as _int4
    con.createArrayOf("int4", new int[][]{{1, 2}, {3, 4}});
    rs = dbmd.getColumns(null, null, "intarraytable", "b");
    assertTrue(rs.next().get());
    assertEquals("_int4", rs.getString("TYPE_NAME")); // even int4[][] is represented as _int4
  }

  @Test
  public void testCustomArrayTypeInfo() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet res = dbmd.getColumns(null, null, "customtable", null);
    assertTrue(res.next().get());
    assertEquals("custom", res.getString("TYPE_NAME"));
    assertTrue(res.next().get());
    assertEquals("_custom", res.getString("TYPE_NAME"));
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_3)) {
      assertTrue(res.next().get());
      assertEquals("__custom", res.getString("TYPE_NAME"));
      assertTrue(res.next().get());
      assertEquals("___custom", res.getString("TYPE_NAME"));
    }
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_3)) {
      con.createArrayOf("custom", new Object[]{});
      res = dbmd.getColumns(null, null, "customtable", null);
      assertTrue(res.next().get());
      assertEquals("custom", res.getString("TYPE_NAME"));
      assertTrue(res.next().get());
      assertEquals("_custom", res.getString("TYPE_NAME"));
      assertTrue(res.next().get());
      assertEquals("__custom", res.getString("TYPE_NAME"));
      assertTrue(res.next().get());
      assertEquals("___custom", res.getString("TYPE_NAME"));
    }
  }

  @Test
  public void testTables() throws Exception {
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    VxResultSet rs = dbmd.getTables(null, null, "metadatates%", new String[]{"TABLE"});
    assertTrue(rs.next().get());
    String tableName = rs.getString("TABLE_NAME").get();
    assertEquals("metadatatest", tableName);
    String tableType = rs.getString("TABLE_TYPE").get();
    assertEquals("TABLE", tableType);
    // There should only be one row returned
    assertTrue("getTables() returned too many rows", rs.next().get() == false);
    rs.close();

    rs = dbmd.getColumns("", "", "meta%", "%");
    assertTrue(rs.next().get());
    assertEquals("metadatatest", rs.getString("TABLE_NAME").get());
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals(java.sql.Types.INTEGER, (Object)rs.getInt("DATA_TYPE").get());

    assertTrue(rs.next().get());
    assertEquals("metadatatest", rs.getString("TABLE_NAME").get());
    assertEquals("name", rs.getString("COLUMN_NAME"));
    assertEquals(java.sql.Types.VARCHAR, (Object)rs.getInt("DATA_TYPE").get());

    assertTrue(rs.next().get());
    assertEquals("metadatatest", rs.getString("TABLE_NAME"));
    assertEquals("updated", rs.getString("COLUMN_NAME"));
    assertEquals(java.sql.Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
  }

  @Test
  public void testCrossReference() throws Exception {
    VxConnection con1 = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con1, "vv", "a int not null, b int not null, primary key ( a, b )");

    VxTestUtil.createTable(con1, "ww",
        "m int not null, n int not null, primary key ( m, n ), foreign key ( m, n ) references vv ( a, b )");


    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    VxResultSet rs = dbmd.getCrossReference(null, "", "vv", null, "", "ww");

    for (int j = 1; rs.next().get(); j++) {

      String pkTableName = rs.getString("PKTABLE_NAME").get();
      assertEquals("vv", pkTableName);

      String pkColumnName = rs.getString("PKCOLUMN_NAME").get();
      assertTrue(pkColumnName.equals("a") || pkColumnName.equals("b"));

      String fkTableName = rs.getString("FKTABLE_NAME").get();
      assertEquals("ww", fkTableName);

      String fkColumnName = rs.getString("FKCOLUMN_NAME").get();
      assertTrue(fkColumnName.equals("m") || fkColumnName.equals("n"));

      String fkName = rs.getString("FK_NAME").get();
      assertEquals("ww_m_fkey", fkName);

      String pkName = rs.getString("PK_NAME").get();
      assertEquals("vv_pkey", pkName);

      int keySeq = rs.getInt("KEY_SEQ").get();
      assertEquals(j, keySeq);
    }


    VxTestUtil.dropTable(con1, "vv");
    VxTestUtil.dropTable(con1, "ww");
    VxTestUtil.closeDB(con1);
  }

  @Test
  public void testForeignKeyActions() throws Exception {
    VxConnection conn = VxTestUtil.openDB().get();
    VxTestUtil.createTable(conn, "pkt", "id int primary key");
    VxTestUtil.createTable(conn, "fkt1",
        "id int references pkt on update restrict on delete cascade");
    VxTestUtil.createTable(conn, "fkt2",
        "id int references pkt on update set null on delete set default");
    VxDatabaseMetaData dbmd = conn.getMetaData();

    VxResultSet rs = dbmd.getImportedKeys(null, "", "fkt1");
    assertTrue(rs.next().get());
    assertEquals(java.sql.DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
    assertEquals(java.sql.DatabaseMetaData.importedKeyCascade, rs.getInt("DELETE_RULE"));
    rs.close();

    rs = dbmd.getImportedKeys(null, "", "fkt2");
    assertTrue(rs.next().get());
    assertEquals(java.sql.DatabaseMetaData.importedKeySetNull, rs.getInt("UPDATE_RULE"));
    assertEquals(java.sql.DatabaseMetaData.importedKeySetDefault, rs.getInt("DELETE_RULE"));
    rs.close();

    VxTestUtil.dropTable(conn, "fkt2");
    VxTestUtil.dropTable(conn, "fkt1");
    VxTestUtil.dropTable(conn, "pkt");
    VxTestUtil.closeDB(conn);
  }

  @Test
  public void testForeignKeysToUniqueIndexes() throws Exception {
    VxConnection con1 = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con1, "pkt",
        "a int not null, b int not null, CONSTRAINT pkt_pk_a PRIMARY KEY (a), CONSTRAINT pkt_un_b UNIQUE (b)");
    VxTestUtil.createTable(con1, "fkt",
        "c int, d int, CONSTRAINT fkt_fk_c FOREIGN KEY (c) REFERENCES pkt(b)");

    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getImportedKeys("", "", "fkt");
    int j = 0;
    for (; rs.next().get(); j++) {
      assertEquals("pkt", rs.getString("PKTABLE_NAME"));
      assertEquals("fkt", rs.getString("FKTABLE_NAME"));
      assertEquals("pkt_un_b", rs.getString("PK_NAME"));
      assertEquals("b", rs.getString("PKCOLUMN_NAME"));
    }
    assertEquals(1, j);

    VxTestUtil.dropTable(con1, "fkt");
    VxTestUtil.dropTable(con1, "pkt");
    con1.close();
  }

  @Test
  public void testMultiColumnForeignKeys() throws Exception {
    VxConnection con1 = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con1, "pkt",
        "a int not null, b int not null, CONSTRAINT pkt_pk PRIMARY KEY (a,b)");
    VxTestUtil.createTable(con1, "fkt",
        "c int, d int, CONSTRAINT fkt_fk_pkt FOREIGN KEY (c,d) REFERENCES pkt(b,a)");

    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getImportedKeys("", "", "fkt");
    int j = 0;
    for (; rs.next().get(); j++) {
      assertEquals("pkt", rs.getString("PKTABLE_NAME"));
      assertEquals("fkt", rs.getString("FKTABLE_NAME"));
      assertEquals(j + 1, rs.getInt("KEY_SEQ"));
      if (j == 0) {
        assertEquals("b", rs.getString("PKCOLUMN_NAME"));
        assertEquals("c", rs.getString("FKCOLUMN_NAME"));
      } else {
        assertEquals("a", rs.getString("PKCOLUMN_NAME"));
        assertEquals("d", rs.getString("FKCOLUMN_NAME"));
      }
    }
    assertEquals(2, j);

    VxTestUtil.dropTable(con1, "fkt");
    VxTestUtil.dropTable(con1, "pkt");
    con1.close();
  }

  @Test
  public void testSameTableForeignKeys() throws Exception {
    VxConnection con1 = VxTestUtil.openDB().get();

    VxTestUtil.createTable(con1, "person",
        "FIRST_NAME character varying(100) NOT NULL," + "LAST_NAME character varying(100) NOT NULL,"
            + "FIRST_NAME_PARENT_1 character varying(100),"
            + "LAST_NAME_PARENT_1 character varying(100),"
            + "FIRST_NAME_PARENT_2 character varying(100),"
            + "LAST_NAME_PARENT_2 character varying(100),"
            + "CONSTRAINT PERSON_pkey PRIMARY KEY (FIRST_NAME , LAST_NAME ),"
            + "CONSTRAINT PARENT_1_fkey FOREIGN KEY (FIRST_NAME_PARENT_1, LAST_NAME_PARENT_1)"
            + "REFERENCES PERSON (FIRST_NAME, LAST_NAME) MATCH SIMPLE "
            + "ON UPDATE CASCADE ON DELETE CASCADE,"
            + "CONSTRAINT PARENT_2_fkey FOREIGN KEY (FIRST_NAME_PARENT_2, LAST_NAME_PARENT_2)"
            + "REFERENCES PERSON (FIRST_NAME, LAST_NAME) MATCH SIMPLE "
            + "ON UPDATE CASCADE ON DELETE CASCADE");


    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getImportedKeys(null, "", "person");

    final List<String> fkNames = new ArrayList<String>();

    int lastFieldCount = -1;
    while (rs.next().get()) {
      // destination table (all foreign keys point to the same)
      String pkTableName = rs.getString("PKTABLE_NAME").get();
      assertEquals("person", pkTableName);

      // destination fields
      String pkColumnName = rs.getString("PKCOLUMN_NAME").get();
      assertTrue("first_name".equals(pkColumnName) || "last_name".equals(pkColumnName));

      // source table (all foreign keys are in the same)
      String fkTableName = rs.getString("FKTABLE_NAME").get();
      assertEquals("person", fkTableName);

      // foreign key name
      String fkName = rs.getString("FK_NAME").get();
      // sequence number within the foreign key
      int seq = rs.getInt("KEY_SEQ").get();
      if (seq == 1) {
        // begin new foreign key
        assertFalse(fkNames.contains(fkName));
        fkNames.add(fkName);
        // all foreign keys have 2 fields
        assertTrue(lastFieldCount < 0 || lastFieldCount == 2);
      } else {
        // continue foreign key, i.e. fkName matches the last foreign key
        assertEquals(fkNames.get(fkNames.size() - 1), fkName);
        // see always increases by 1
        assertEquals(seq, lastFieldCount + 1);
      }
      lastFieldCount = seq;
    }
    // there's more than one foreign key from a table to another
    assertEquals(2, fkNames.size());

    VxTestUtil.dropTable(con1, "person");
    VxTestUtil.closeDB(con1);


  }

  @Test
  public void testForeignKeys() throws Exception {
    VxConnection con1 = VxTestUtil.openDB().get();
    VxTestUtil.createTable(con1, "people", "id int4 primary key, name text");
    VxTestUtil.createTable(con1, "policy", "id int4 primary key, name text");

    VxTestUtil.createTable(con1, "users",
        "id int4 primary key, people_id int4, policy_id int4,"
            + "CONSTRAINT people FOREIGN KEY (people_id) references people(id),"
            + "constraint policy FOREIGN KEY (policy_id) references policy(id)");


    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    VxResultSet rs = dbmd.getImportedKeys(null, "", "users");
    int j = 0;
    for (; rs.next().get(); j++) {

      String pkTableName = rs.getString("PKTABLE_NAME").get();
      assertTrue(pkTableName.equals("people") || pkTableName.equals("policy"));

      String pkColumnName = rs.getString("PKCOLUMN_NAME").get();
      assertEquals("id", pkColumnName);

      String fkTableName = rs.getString("FKTABLE_NAME").get();
      assertEquals("users", fkTableName);

      String fkColumnName = rs.getString("FKCOLUMN_NAME").get();
      assertTrue(fkColumnName.equals("people_id") || fkColumnName.equals("policy_id"));

      String fkName = rs.getString("FK_NAME").get();
      assertTrue(fkName.startsWith("people") || fkName.startsWith("policy"));

      String pkName = rs.getString("PK_NAME").get();
      assertTrue(pkName.equals("people_pkey") || pkName.equals("policy_pkey"));

    }

    assertEquals(2, j);

    rs = dbmd.getExportedKeys(null, "", "people");

    // this is hacky, but it will serve the purpose
    assertTrue(rs.next().get());

    assertEquals("people", rs.getString("PKTABLE_NAME"));
    assertEquals("id", rs.getString("PKCOLUMN_NAME"));

    assertEquals("users", rs.getString("FKTABLE_NAME"));
    assertEquals("people_id", rs.getString("FKCOLUMN_NAME"));

    assertTrue(rs.getString("FK_NAME").get().startsWith("people"));


    VxTestUtil.dropTable(con1, "users");
    VxTestUtil.dropTable(con1, "people");
    VxTestUtil.dropTable(con1, "policy");
    VxTestUtil.closeDB(con1);
  }

  @Test
  public void testColumns() throws SQLException, InterruptedException, ExecutionException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getColumns(null, null, "pg_class", null);
    rs.close();
  }

  @Test
  public void testDroppedColumns() throws SQLException, InterruptedException, ExecutionException {
    if (!VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4)) {
      return;
    }

    VxStatement stmt = con.createStatement();
    stmt.execute("ALTER TABLE metadatatest DROP name");
    stmt.execute("ALTER TABLE metadatatest DROP colour");
    stmt.close();

    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getColumns(null, null, "metadatatest", null);

    assertTrue(rs.next().get());
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));

    assertTrue(rs.next().get());
    assertEquals("updated", rs.getString("COLUMN_NAME"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));

    assertTrue(rs.next().get());
    assertEquals("quest", rs.getString("COLUMN_NAME"));
    assertEquals(3, rs.getInt("ORDINAL_POSITION"));

    rs.close();

    rs = dbmd.getColumns(null, null, "metadatatest", "quest");
    assertTrue(rs.next().get());
    assertEquals("quest", rs.getString("COLUMN_NAME"));
    assertEquals(3, rs.getInt("ORDINAL_POSITION"));
    assertFalse(rs.next().get());
    rs.close();

    /* getFunctionColumns also has to be aware of dropped columns
       add this in here to make sure it can deal with them
     */
    rs = dbmd.getFunctionColumns(null, null, "f4", null);
    assertTrue(rs.next().get());

    assertTrue(rs.next().get());
    assertEquals("id", rs.getString(4));

    assertTrue(rs.next().get());
    assertEquals("updated", rs.getString(4));


    rs.close();

  }

  @Test
  public void testSerialColumns() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getColumns(null, null, "sercoltest", null);
    int rownum = 0;
    while (rs.next().get()) {
      assertEquals("sercoltest", rs.getString("TABLE_NAME"));
      assertEquals(rownum + 1, rs.getInt("ORDINAL_POSITION"));
      if (rownum == 0) {
        assertEquals("int4", rs.getString("TYPE_NAME"));
      } else if (rownum == 1) {
        assertEquals("serial", rs.getString("TYPE_NAME"));
      } else if (rownum == 2) {
        assertEquals("bigserial", rs.getString("TYPE_NAME"));
      }
      rownum++;
    }
    assertEquals(3, rownum);
    rs.close();
  }

  @Test
  public void testColumnPrivileges() throws SQLException, InterruptedException, ExecutionException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getColumnPrivileges(null, null, "pg_statistic", null);
    rs.close();
  }

  @Test
  public void testTablePrivileges() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getTablePrivileges(null, null, "metadatatest");
    boolean l_foundSelect = false;
    while (rs.next().get()) {
      if (rs.getString("GRANTEE").equals(VxTestUtil.getUser())
          && rs.getString("PRIVILEGE").equals("SELECT")) {
        l_foundSelect = true;
      }
    }
    rs.close();
    // Test that the table owner has select priv
    assertTrue("Couldn't find SELECT priv on table metadatatest for " + VxTestUtil.getUser(),
        l_foundSelect);
  }

  @Test
  public void testNoTablePrivileges() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    stmt.execute("REVOKE ALL ON metadatatest FROM PUBLIC");
    stmt.execute("REVOKE ALL ON metadatatest FROM " + VxTestUtil.getUser());
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getTablePrivileges(null, null, "metadatatest");
    assertTrue(!rs.next().get());
  }

  @Test
  public void testPrimaryKeys() throws SQLException, InterruptedException, ExecutionException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getPrimaryKeys(null, null, "pg_class");
    rs.close();
  }

  @Test
  public void testIndexInfo() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    stmt.execute("create index idx_id on metadatatest (id)");
    stmt.execute("create index idx_func_single on metadatatest (upper(colour))");
    stmt.execute("create unique index idx_un_id on metadatatest(id)");
    stmt.execute("create index idx_func_multi on metadatatest (upper(colour), upper(quest))");
    stmt.execute("create index idx_func_mixed on metadatatest (colour, upper(quest))");

    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getIndexInfo(null, null, "metadatatest", false, false);

    assertTrue(rs.next().get());
    assertEquals("idx_un_id", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertTrue(!rs.getBoolean("NON_UNIQUE").get());

    assertTrue(rs.next().get());
    assertEquals("idx_func_mixed", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("colour", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next().get());
    assertEquals("idx_func_mixed", rs.getString("INDEX_NAME"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(quest)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next().get());
    assertEquals("idx_func_multi", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(colour)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next().get());
    assertEquals("idx_func_multi", rs.getString("INDEX_NAME"));
    assertEquals(2, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(quest)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next().get());
    assertEquals("idx_func_single", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("upper(colour)", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next().get());
    assertEquals("idx_id", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertTrue(rs.getBoolean("NON_UNIQUE").get());

    assertTrue(!rs.next().get());

    rs.close();
  }

  @Test
  public void testNotNullDomainColumn() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getColumns("", "", "domaintable", "");
    assertTrue(rs.next().get());
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals("NO", rs.getString("IS_NULLABLE"));
    assertTrue(!rs.next().get());
  }

  @Test
  public void testAscDescIndexInfo() throws SQLException, InterruptedException, ExecutionException {
    if (!VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_3)) {
      return;
    }

    VxStatement stmt = con.createStatement();
    stmt.execute("CREATE INDEX idx_a_d ON metadatatest (id ASC, quest DESC)");
    stmt.close();

    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getIndexInfo(null, null, "metadatatest", false, false);

    assertTrue(rs.next().get());
    assertEquals("idx_a_d", rs.getString("INDEX_NAME"));
    assertEquals("id", rs.getString("COLUMN_NAME"));
    assertEquals("A", rs.getString("ASC_OR_DESC"));


    assertTrue(rs.next().get());
    assertEquals("idx_a_d", rs.getString("INDEX_NAME"));
    assertEquals("quest", rs.getString("COLUMN_NAME"));
    assertEquals("D", rs.getString("ASC_OR_DESC"));
  }

  @Test
  public void testPartialIndexInfo() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = con.createStatement();
    stmt.execute("create index idx_p_name_id on metadatatest (name) where id > 5");
    stmt.close();

    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getIndexInfo(null, null, "metadatatest", false, false);

    assertTrue(rs.next().get());
    assertEquals("idx_p_name_id", rs.getString("INDEX_NAME"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertEquals("name", rs.getString("COLUMN_NAME"));
    assertEquals("(id > 5)", rs.getString("FILTER_CONDITION"));
    assertTrue(rs.getBoolean("NON_UNIQUE").get());

    rs.close();
  }

  @Test
  public void testTableTypes() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getTableTypes();
    rs.close();
  }

  @Test
  public void testFuncWithoutNames() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getProcedureColumns(null, null, "f1", null);

    assertTrue(rs.next().get());
    assertEquals("returnValue", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnReturn, rs.getInt(5));

    assertTrue(rs.next().get());
    assertEquals("$1", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("$2", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(!rs.next().get());

    rs.close();
  }

  @Test
  public void testFuncWithNames() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getProcedureColumns(null, null, "f2", null);

    assertTrue(rs.next().get());

    assertTrue(rs.next().get());
    assertEquals("a", rs.getString(4));

    assertTrue(rs.next().get());
    assertEquals("b", rs.getString(4));

    assertTrue(!rs.next().get());

    rs.close();
  }

  @Test
  public void testFuncWithDirection() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getProcedureColumns(null, null, "f3", null);

    assertTrue(rs.next().get());
    assertEquals("a", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("b", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnInOut, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("c", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnOut, rs.getInt(5));
    assertEquals(Types.TIMESTAMP, rs.getInt(6));

    rs.close();
  }

  @Test
  public void testFuncReturningComposite() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getProcedureColumns(null, null, "f4", null);

    assertTrue(rs.next().get());
    assertEquals("$1", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnIn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("id", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("name", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("updated", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.TIMESTAMP, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("colour", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(rs.next().get());
    assertEquals("quest", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnResult, rs.getInt(5));
    assertEquals(Types.VARCHAR, rs.getInt(6));

    assertTrue(!rs.next().get());
    rs.close();
  }

  @Test
  public void testFuncReturningTable() throws Exception {
    if (!VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4)) {
      return;
    }
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getProcedureColumns(null, null, "f5", null);
    assertTrue(rs.next().get());
    assertEquals("returnValue", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnReturn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));
    assertTrue(rs.next().get());
    assertEquals("i", rs.getString(4));
    assertEquals(java.sql.DatabaseMetaData.procedureColumnReturn, rs.getInt(5));
    assertEquals(Types.INTEGER, rs.getInt(6));
    assertTrue(!rs.next().get());
    rs.close();
  }

  @Test
  public void testVersionColumns() throws SQLException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getVersionColumns(null, null, "pg_class");
    rs.close();
  }

  @Test
  public void testBestRowIdentifier() throws SQLException, InterruptedException, ExecutionException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs =
        dbmd.getBestRowIdentifier(null, null, "pg_type", java.sql.DatabaseMetaData.bestRowSession, false);
    rs.close();
  }

  @Test
  public void testProcedures() throws SQLException, InterruptedException, ExecutionException {
    // At the moment just test that no exceptions are thrown KJ
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);
    VxResultSet rs = dbmd.getProcedures(null, null, null);
    rs.close();
  }

  @Test
  public void testCatalogs() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getCatalogs();
    assertTrue(rs.next().get());
    assertEquals(con.getCatalog(), rs.getString(1));
    assertTrue(!rs.next().get());
  }

  @Test
  public void testSchemas() throws Exception {
    VxDatabaseMetaData dbmd = con.getMetaData();
    assertNotNull(dbmd);

    VxResultSet rs = dbmd.getSchemas();
    boolean foundPublic = false;
    boolean foundEmpty = false;
    boolean foundPGCatalog = false;
    int count;

    for (count = 0; rs.next().get(); count++) {
      String schema = rs.getString("TABLE_SCHEM").get();
      if ("public".equals(schema)) {
        foundPublic = true;
      } else if ("".equals(schema)) {
        foundEmpty = true;
      } else if ("pg_catalog".equals(schema)) {
        foundPGCatalog = true;
      }
    }
    rs.close();
    assertTrue(count >= 2);
    assertTrue(foundPublic);
    assertTrue(foundPGCatalog);
    assertTrue(!foundEmpty);
  }

  @Test
  public void testEscaping() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getTables(null, null, "a'", new String[]{"TABLE"});
    assertTrue(rs.next().get());
    rs = dbmd.getTables(null, null, "a\\\\", new String[]{"TABLE"});
    assertTrue(rs.next().get());
    rs = dbmd.getTables(null, null, "a\\", new String[]{"TABLE"});
    assertTrue(!rs.next().get());
  }

  @Test
  public void testSearchStringEscape() throws Exception {
    VxDatabaseMetaData dbmd = con.getMetaData();
    String pattern = dbmd.getSearchStringEscape() + "_";
    VxPreparedStatement pstmt = con.prepareStatement("SELECT 'a' LIKE ?, '_' LIKE ?");
    pstmt.setString(1, pattern);
    pstmt.setString(2, pattern);
    VxResultSet rs = pstmt.executeQuery().get();
    assertTrue(rs.next().get());
    assertTrue(!rs.getBoolean(1).get());
    assertTrue(rs.getBoolean(2).get());
    rs.close();
    pstmt.close();
  }

  @Test
  public void testGetUDTQualified() throws Exception {
    VxStatement stmt = null;
    try {
      stmt = con.createStatement();
      stmt.execute("create schema jdbc");
      stmt.execute("create type jdbc.testint8 as (i int8)");
      VxDatabaseMetaData dbmd = con.getMetaData();
      VxResultSet rs = dbmd.getUDTs(null, null, "jdbc.testint8", null);
      assertTrue(rs.next().get());
      String cat;
      String schema;
      String typeName;
      String remarks;
      String className;
      int dataType;
      int baseType;

      cat = rs.getString("type_cat").get();
      schema = rs.getString("type_schem").get();
      typeName = rs.getString("type_name").get();
      className = rs.getString("class_name").get();
      dataType = rs.getInt("data_type").get();
      remarks = rs.getString("remarks").get();
      baseType = rs.getInt("base_type").get();
      assertEquals("type name ", "testint8", typeName);
      assertEquals("schema name ", "jdbc", schema);

      // now test to see if the fully qualified stuff works as planned
      rs = dbmd.getUDTs("catalog", "public", "catalog.jdbc.testint8", null);
      assertTrue(rs.next().get());
      cat = rs.getString("type_cat").get();
      schema = rs.getString("type_schem").get();
      typeName = rs.getString("type_name").get();
      className = rs.getString("class_name").get();
      dataType = rs.getInt("data_type").get();
      remarks = rs.getString("remarks").get();
      baseType = rs.getInt("base_type").get();
      assertEquals("type name ", "testint8", typeName);
      assertEquals("schema name ", "jdbc", schema);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        stmt = con.createStatement();
        stmt.execute("drop type jdbc.testint8");
        stmt.execute("drop schema jdbc");
      } catch (Exception ex) {
      }
    }

  }

  @Test
  public void testGetUDT1() throws Exception {
    try {
      VxStatement stmt = con.createStatement();
      stmt.execute("create domain testint8 as int8");
      stmt.execute("comment on domain testint8 is 'jdbc123'");
      VxDatabaseMetaData dbmd = con.getMetaData();
      VxResultSet rs = dbmd.getUDTs(null, null, "testint8", null);
      assertTrue(rs.next().get());

      String cat = rs.getString("type_cat").get();
      String schema = rs.getString("type_schem").get();
      String typeName = rs.getString("type_name").get();
      String className = rs.getString("class_name").get();
      int dataType = rs.getInt("data_type").get();
      String remarks = rs.getString("remarks").get();

      int baseType = rs.getInt("base_type").get();
      assertTrue("base type", !rs.wasNull());
      assertEquals("data type", Types.DISTINCT, dataType);
      assertEquals("type name ", "testint8", typeName);
      assertEquals("remarks", "jdbc123", remarks);

    } finally {
      try {
        VxStatement stmt = con.createStatement();
        stmt.execute("drop domain testint8");
      } catch (Exception ex) {
      }
    }
  }


  @Test
  public void testGetUDT2() throws Exception {
    try {
      VxStatement stmt = con.createStatement();
      stmt.execute("create domain testint8 as int8");
      stmt.execute("comment on domain testint8 is 'jdbc123'");
      VxDatabaseMetaData dbmd = con.getMetaData();
      VxResultSet rs = dbmd.getUDTs(null, null, "testint8", new int[]{Types.DISTINCT, Types.STRUCT});
      assertTrue(rs.next().get());
      String typeName;

      String cat = rs.getString("type_cat").get();
      String schema = rs.getString("type_schem").get();
      typeName = rs.getString("type_name").get();
      String className = rs.getString("class_name").get();
      int dataType = rs.getInt("data_type").get();
      String remarks = rs.getString("remarks").get();

      int baseType = rs.getInt("base_type").get();
      assertTrue("base type", !rs.wasNull());
      assertEquals("data type", Types.DISTINCT, dataType);
      assertEquals("type name ", "testint8", typeName);
      assertEquals("remarks", "jdbc123", remarks);

    } finally {
      try {
        VxStatement stmt = con.createStatement();
        stmt.execute("drop domain testint8");
      } catch (Exception ex) {
      }
    }
  }

  @Test
  public void testGetUDT3() throws Exception {
    try {
      VxStatement stmt = con.createStatement();
      stmt.execute("create domain testint8 as int8");
      stmt.execute("comment on domain testint8 is 'jdbc123'");
      VxDatabaseMetaData dbmd = con.getMetaData();
      VxResultSet rs = dbmd.getUDTs(null, null, "testint8", new int[]{Types.DISTINCT});
      assertTrue(rs.next().get());

      String cat = rs.getString("type_cat").get();
      String schema = rs.getString("type_schem").get();
      String typeName = rs.getString("type_name").get();
      String className = rs.getString("class_name").get();
      int dataType = rs.getInt("data_type").get();
      String remarks = rs.getString("remarks").get();

      int baseType = rs.getInt("base_type").get();
      assertTrue("base type", !rs.wasNull());
      assertEquals("data type", Types.DISTINCT, dataType);
      assertEquals("type name ", "testint8", typeName);
      assertEquals("remarks", "jdbc123", remarks);

    } finally {
      try {
        VxStatement stmt = con.createStatement();
        stmt.execute("drop domain testint8");
      } catch (Exception ex) {
      }
    }
  }

  @Test
  public void testGetUDT4() throws Exception {
    try {
      VxStatement stmt = con.createStatement();
      stmt.execute("create type testint8 as (i int8)");
      VxDatabaseMetaData dbmd = con.getMetaData();
      VxResultSet rs = dbmd.getUDTs(null, null, "testint8", null);
      assertTrue(rs.next().get());

      String cat = rs.getString("type_cat").get();
      String schema = rs.getString("type_schem").get();
      String typeName = rs.getString("type_name").get();
      String className = rs.getString("class_name").get();
      int dataType = rs.getInt("data_type").get();
      String remarks = rs.getString("remarks").get();

      int baseType = rs.getInt("base_type").get();
      assertTrue("base type", rs.wasNull());
      assertEquals("data type", Types.STRUCT, dataType);
      assertEquals("type name ", "testint8", typeName);

    } finally {
      try {
        VxStatement stmt = con.createStatement();
        stmt.execute("drop type testint8");
      } catch (Exception ex) {
      }
    }
  }

  @Test
  public void testTypes() throws SQLException, InterruptedException, ExecutionException {
    // https://www.postgresql.org/docs/8.2/static/datatype.html
    List<String> stringTypeList = new ArrayList<String>();
    stringTypeList.addAll(Arrays.asList("bit",
            "bool",
            "box",
            "bytea",
            "char",
            "cidr",
            "circle",
            "date",
            "float4",
            "float8",
            "inet",
            "int2",
            "int4",
            "int8",
            "interval",
            "line",
            "lseg",
            "macaddr",
            "money",
            "numeric",
            "path",
            "point",
            "polygon",
            "text",
            "time",
            "timestamp",
            "timestamptz",
            "timetz",
            "varbit",
            "varchar"));
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v8_3)) {
      stringTypeList.add("tsquery");
      stringTypeList.add("tsvector");
      stringTypeList.add("txid_snapshot");
      stringTypeList.add("uuid");
      stringTypeList.add("xml");
    }
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v9_2)) {
      stringTypeList.add("json");
    }
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v9_4)) {
      stringTypeList.add("jsonb");
      stringTypeList.add("pg_lsn");
    }

    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getTypeInfo();
    List<String> types = new ArrayList<String>();

    while (rs.next().get()) {
      types.add(rs.getString("TYPE_NAME").get());
    }
    for (String typeName : stringTypeList) {
      assertTrue(types.contains(typeName));
    }

  }

  @Test
  public void testTypeInfoSigned() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getTypeInfo();
    while (rs.next().get()) {
      if ("int4".equals(rs.getString("TYPE_NAME"))) {
        assertEquals(false, rs.getBoolean("UNSIGNED_ATTRIBUTE"));
      } else if ("float8".equals(rs.getString("TYPE_NAME"))) {
        assertEquals(false, rs.getBoolean("UNSIGNED_ATTRIBUTE"));
      } else if ("text".equals(rs.getString("TYPE_NAME"))) {
        assertEquals(true, rs.getBoolean("UNSIGNED_ATTRIBUTE"));
      }
    }
  }

  @Test
  public void testTypeInfoQuoting() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getTypeInfo();
    while (rs.next().get()) {
      if ("int4".equals(rs.getString("TYPE_NAME"))) {
        assertNull(rs.getString("LITERAL_PREFIX"));
      } else if ("text".equals(rs.getString("TYPE_NAME"))) {
        assertEquals("'", rs.getString("LITERAL_PREFIX"));
        assertEquals("'", rs.getString("LITERAL_SUFFIX"));
      }
    }
  }

  @Test
  public void testInformationAboutArrayTypes() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    VxResultSet rs = dbmd.getColumns("", "", "arraytable", "");
    assertTrue(rs.next().get());
    assertEquals("a", rs.getString("COLUMN_NAME"));
    assertEquals(5, rs.getInt("COLUMN_SIZE"));
    assertEquals(2, rs.getInt("DECIMAL_DIGITS"));
    assertTrue(rs.next().get());
    assertEquals("b", rs.getString("COLUMN_NAME"));
    assertEquals(100, rs.getInt("COLUMN_SIZE"));
    assertTrue(!rs.next().get());
  }

  @Test
  public void testPartitionedTables() throws SQLException, InterruptedException, ExecutionException {
    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v10)) {
      VxStatement stmt = null;
      try {
        stmt = con.createStatement();
        stmt.execute(
            "CREATE TABLE measurement (logdate date not null,peaktemp int,unitsales int ) PARTITION BY RANGE (logdate);");
        VxDatabaseMetaData dbmd = con.getMetaData();
        VxResultSet rs = dbmd.getTables("", "", "measurement", new String[]{"TABLE"});
        assertTrue(rs.next().get());
        assertEquals("measurement", rs.getString("table_name"));

      } finally {
        if (stmt != null) {
          stmt.execute("drop table measurement");
          stmt.close();
        }
      }
    }
  }

  @Test
  public void testIdentityColumns() throws SQLException, InterruptedException, ExecutionException {
    if ( VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v10) ) {
      VxStatement stmt = null;
      try {
        stmt = con.createStatement();
        stmt.execute("CREATE TABLE test_new ("
            + "id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,"
            + "payload text)");
        VxDatabaseMetaData dbmd = con.getMetaData();
        VxResultSet rs = dbmd.getColumns("", "", "test_new", "id");
        assertTrue(rs.next().get());
        assertEquals(rs.getString("COLUMN_NAME"), "id");
        assertTrue(rs.getBoolean("IS_AUTOINCREMENT").get());

      } finally {
        if ( stmt != null ) {
          stmt.execute( "drop table test_new");
          stmt.close();
        }
      }
    }

  }

  @Test
  public void testGetSQLKeywords() throws SQLException, InterruptedException, ExecutionException {
    VxDatabaseMetaData dbmd = con.getMetaData();
    String keywords = dbmd.getSQLKeywords();

    // We don't want SQL:2003 keywords returned, so check for that.
    String sql2003 = "a,abs,absolute,action,ada,add,admin,after,all,allocate,alter,always,and,any,are,"
        + "array,as,asc,asensitive,assertion,assignment,asymmetric,at,atomic,attribute,attributes,"
        + "authorization,avg,before,begin,bernoulli,between,bigint,binary,blob,boolean,both,breadth,by,"
        + "c,call,called,cardinality,cascade,cascaded,case,cast,catalog,catalog_name,ceil,ceiling,chain,"
        + "char,char_length,character,character_length,character_set_catalog,character_set_name,"
        + "character_set_schema,characteristics,characters,check,checked,class_origin,clob,close,"
        + "coalesce,cobol,code_units,collate,collation,collation_catalog,collation_name,collation_schema,"
        + "collect,column,column_name,command_function,command_function_code,commit,committed,condition,"
        + "condition_number,connect,connection_name,constraint,constraint_catalog,constraint_name,"
        + "constraint_schema,constraints,constructors,contains,continue,convert,corr,corresponding,count,"
        + "covar_pop,covar_samp,create,cross,cube,cume_dist,current,current_collation,current_date,"
        + "current_default_transform_group,current_path,current_role,current_time,current_timestamp,"
        + "current_transform_group_for_type,current_user,cursor,cursor_name,cycle,data,date,datetime_interval_code,"
        + "datetime_interval_precision,day,deallocate,dec,decimal,declare,default,defaults,deferrable,"
        + "deferred,defined,definer,degree,delete,dense_rank,depth,deref,derived,desc,describe,"
        + "descriptor,deterministic,diagnostics,disconnect,dispatch,distinct,domain,double,drop,dynamic,"
        + "dynamic_function,dynamic_function_code,each,element,else,end,end-exec,equals,escape,every,"
        + "except,exception,exclude,excluding,exec,execute,exists,exp,external,extract,false,fetch,filter,"
        + "final,first,float,floor,following,for,foreign,fortran,found,free,from,full,function,fusion,"
        + "g,general,get,global,go,goto,grant,granted,group,grouping,having,hierarchy,hold,hour,identity,"
        + "immediate,implementation,in,including,increment,indicator,initially,inner,inout,input,"
        + "insensitive,insert,instance,instantiable,int,integer,intersect,intersection,interval,into,"
        + "invoker,is,isolation,join,k,key,key_member,key_type,language,large,last,lateral,leading,left,"
        + "length,level,like,ln,local,localtime,localtimestamp,locator,lower,m,map,match,matched,max,"
        + "maxvalue,member,merge,message_length,message_octet_length,message_text,method,min,minute,"
        + "minvalue,mod,modifies,module,month,more,multiset,mumps,name,names,national,natural,nchar,"
        + "nclob,nesting,new,next,no,none,normalize,normalized,not,null,nullable,nullif,nulls,number,"
        + "numeric,object,octet_length,octets,of,old,on,only,open,option,options,or,order,ordering,"
        + "ordinality,others,out,outer,output,over,overlaps,overlay,overriding,pad,parameter,parameter_mode,"
        + "parameter_name,parameter_ordinal_position,parameter_specific_catalog,parameter_specific_name,"
        + "parameter_specific_schema,partial,partition,pascal,path,percent_rank,percentile_cont,"
        + "percentile_disc,placing,pli,position,power,preceding,precision,prepare,preserve,primary,"
        + "prior,privileges,procedure,public,range,rank,read,reads,real,recursive,ref,references,"
        + "referencing,regr_avgx,regr_avgy,regr_count,regr_intercept,regr_r2,regr_slope,regr_sxx,"
        + "regr_sxy,regr_syy,relative,release,repeatable,restart,result,return,returned_cardinality,"
        + "returned_length,returned_octet_length,returned_sqlstate,returns,revoke,right,role,rollback,"
        + "rollup,routine,routine_catalog,routine_name,routine_schema,row,row_count,row_number,rows,"
        + "savepoint,scale,schema,schema_name,scope_catalog,scope_name,scope_schema,scroll,search,second,"
        + "section,security,select,self,sensitive,sequence,serializable,server_name,session,session_user,"
        + "set,sets,similar,simple,size,smallint,some,source,space,specific,specific_name,specifictype,sql,"
        + "sqlexception,sqlstate,sqlwarning,sqrt,start,state,statement,static,stddev_pop,stddev_samp,"
        + "structure,style,subclass_origin,submultiset,substring,sum,symmetric,system,system_user,table,"
        + "table_name,tablesample,temporary,then,ties,time,timestamp,timezone_hour,timezone_minute,to,"
        + "top_level_count,trailing,transaction,transaction_active,transactions_committed,"
        + "transactions_rolled_back,transform,transforms,translate,translation,treat,trigger,trigger_catalog,"
        + "trigger_name,trigger_schema,trim,true,type,uescape,unbounded,uncommitted,under,union,unique,"
        + "unknown,unnamed,unnest,update,upper,usage,user,user_defined_type_catalog,user_defined_type_code,"
        + "user_defined_type_name,user_defined_type_schema,using,value,values,var_pop,var_samp,varchar,"
        + "varying,view,when,whenever,where,width_bucket,window,with,within,without,work,write,year,zone";

    String[] excludeSQL2003 = sql2003.split(",");
    String[] returned = keywords.split(",");
    Set<String> returnedSet = new HashSet<String>(Arrays.asList(returned));
    Assert.assertEquals("Returned keywords should be unique", returnedSet.size(), returned.length);

    for (String s : excludeSQL2003) {
      assertFalse("Keyword from SQL:2003 \"" + s + "\" found", returnedSet.contains(s));
    }

    if (VxTestUtil.haveMinimumServerVersion(con, ServerVersion.v9_0)) {
      Assert.assertTrue("reindex should be in keywords", returnedSet.contains("reindex"));
    }
  }

}

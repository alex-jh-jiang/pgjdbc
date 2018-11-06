/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import java.sql.SQLException;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;

public class VxJdbcBlackHole {
  public static void close(VxConnection con) {
    try {
      if (con != null) {
        con.close();
      }
    } catch (SQLException e) {
      /* ignore for now */
    }
  }

  public static void close(VxStatement s) {
    try {
      if (s != null) {
        s.close();
      }
    } catch (SQLException e) {
      /* ignore for now */
    }
  }

  public static void close(VxResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      /* ignore for now */
    }
  }
}

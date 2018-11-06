/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxDatabaseMetaData;

import org.junit.Test;

public class VxTestACL {

  @Test
  public void testParseACL() {
    VxConnection _a = null;
    VxDatabaseMetaData a = new VxDatabaseMetaData(_a) {
    };
    a.parseACL("{jurka=arwdRxt/jurka,permuser=rw*/jurka}", "jurka");
    a.parseACL("{jurka=a*r*w*d*R*x*t*/jurka,permuser=rw*/jurka}", "jurka");
    a.parseACL("{=,jurka=arwdRxt,permuser=rw}", "jurka");
    a.parseACL("{jurka=arwdRxt/jurka,permuser=rw*/jurka,grantuser=w/permuser}", "jurka");
    a.parseACL("{jurka=a*r*w*d*R*x*t*/jurka,permuser=rw*/jurka,grantuser=w/permuser}", "jurka");
    a.parseACL(
        "{jurka=arwdRxt/jurka,permuser=rw*/jurka,grantuser=w/permuser,\"group permgroup=a/jurka\"}",
        "jurka");
    a.parseACL(
        "{jurka=a*r*w*d*R*x*t*/jurka,permuser=rw*/jurka,grantuser=w/permuser,\"group permgroup=a/jurka\"}",
        "jurka");
  }
}

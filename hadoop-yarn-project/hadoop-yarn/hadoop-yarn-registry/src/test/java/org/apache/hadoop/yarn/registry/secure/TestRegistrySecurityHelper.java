/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.registry.secure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Test for registry security operations
 */
public class TestRegistrySecurityHelper extends Assert {

  public static final String YARN_EXAMPLE_COM = "yarn@example.com";
  public static final String SASL_YARN_EXAMPLE_COM =
      "sasl:" + YARN_EXAMPLE_COM;
  public static final String MAPRED_EXAMPLE_COM = "mapred@example.com";
  public static final String SASL_MAPRED_EXAMPLE_COM =
      "sasl:" + MAPRED_EXAMPLE_COM;
  public static final String SASL_MAPRED_APACHE = "sasl:mapred@APACHE";
  public static final String DIGEST_F0AF = "digest:f0afbeeb00baa";
  public static final String SASL_YARN_SHORT = "sasl:yarn@";
  public static final String SASL_MAPRED_SHORT = "sasl:mapred@";
  public static final String REALM_EXAMPLE_COM = "example.com";
  private static RegistrySecurity registrySecurity;

  @BeforeClass
  public static void setupTestRegistrySecurityHelper() throws IOException {
    Configuration conf = new Configuration();
    registrySecurity = new RegistrySecurity(conf);
  }

  @Test
  public void testACLSplitRealmed() throws Throwable {
    List<String> pairs =
        registrySecurity.splitAclPairs(
            SASL_YARN_EXAMPLE_COM +
            ", " +
            SASL_MAPRED_EXAMPLE_COM,
            "");

    assertEquals(SASL_YARN_EXAMPLE_COM, pairs.get(0));
    assertEquals(SASL_MAPRED_EXAMPLE_COM, pairs.get(1));
  }


  @Test
  public void testBuildAclsRealmed() throws Throwable {
    List<ACL> acls = registrySecurity.buildACLs(
        SASL_YARN_EXAMPLE_COM +
        ", " +
        SASL_MAPRED_EXAMPLE_COM,
        "",
        ZooDefs.Perms.ALL);
    assertEquals(YARN_EXAMPLE_COM, acls.get(0).getId().getId());
    assertEquals(MAPRED_EXAMPLE_COM, acls.get(1).getId().getId());
  }

  @Test
  public void testACLSplitNoRealm() throws Throwable {
    List<String> pairs =
        registrySecurity.splitAclPairs(
            SASL_YARN_SHORT +
            ", " +
            SASL_MAPRED_SHORT,
            REALM_EXAMPLE_COM);

    assertEquals(SASL_YARN_EXAMPLE_COM, pairs.get(0));
    assertEquals(SASL_MAPRED_EXAMPLE_COM, pairs.get(1));
  }
  
  @Test
  public void testBuildAclsNoRealm() throws Throwable {
    List<ACL> acls = registrySecurity.buildACLs(
        SASL_YARN_SHORT +
        ", " +
        SASL_MAPRED_SHORT,
        REALM_EXAMPLE_COM, ZooDefs.Perms.ALL);

    assertEquals(YARN_EXAMPLE_COM, acls.get(0).getId().getId());
    assertEquals(MAPRED_EXAMPLE_COM, acls.get(1).getId().getId());
  }

  @Test
  public void testACLSplitNullRealm() throws Throwable {
    List<String> pairs =
        registrySecurity.splitAclPairs(
            SASL_YARN_SHORT +
            ", " +
            SASL_MAPRED_SHORT,
            "");

    assertEquals(SASL_YARN_SHORT, pairs.get(0));
    assertEquals(SASL_MAPRED_SHORT, pairs.get(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildAclsNullRealm() throws Throwable {
    registrySecurity.buildACLs(
        SASL_YARN_SHORT +
        ", " +
        SASL_MAPRED_SHORT,
        "", ZooDefs.Perms.ALL);
    fail("");

  }

  @Test
  public void testACLSplitMixed() throws Throwable {
    List<String> pairs =
        registrySecurity.splitAclPairs(
            SASL_YARN_SHORT +
            ", " +
            SASL_MAPRED_APACHE +
            ", ,," +
            DIGEST_F0AF,
            REALM_EXAMPLE_COM);

    assertEquals(SASL_YARN_EXAMPLE_COM, pairs.get(0));
    assertEquals(SASL_MAPRED_APACHE, pairs.get(1));
    assertEquals(DIGEST_F0AF, pairs.get(2));
  }


}

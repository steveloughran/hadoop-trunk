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



import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

/**
 * Verify that the Mini ZK service can be started up securely
 */
public class TestSecureLogins extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureLogins.class);


  @Test
  public void testHasRealm() throws Throwable {
    assertNotNull(getRealm());
    LOG.info("ZK principal = {}", getPrincipalAndRealm(ZOOKEEPER));
  }


  @Test
  public void testClientLogin() throws Throwable {
    loginAsClient(ALICE, "", keytab_alice);
  }


  @Test
  public void testServerLogin() throws Throwable {
    String name = "";
    assertNull("already logged in", loginContext);
    loginContext = null;
    String principalAndRealm = getPrincipalAndRealm(ZOOKEEPER);
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(ZOOKEEPER));
    Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());
    loginContext = new LoginContext(name, subject, null,
        KerberosConfiguration.createServerConfig(ZOOKEEPER, keytab_zk));
    loginContext.login();
  }



}

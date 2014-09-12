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
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.registry.AbstractRegistryTest;
import org.apache.hadoop.yarn.registry.RegistryTestHelper;
import org.apache.hadoop.yarn.registry.server.services.AddingCompositeService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Add kerberos tests. This is based on the (JUnit3) KerberosSecurityTestcase
 * and its test case, <code>TestMiniKdc</code>
 */
public class AbstractSecureRegistryTest extends RegistryTestHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSecureRegistryTest.class);


  private static final AddingCompositeService servicesToTeardown =
      new AddingCompositeService("teardown");

  // static initializer guarantees it is always started
  // ahead of any @BeforeClass methods
  static {
    servicesToTeardown.init(new Configuration());
    servicesToTeardown.start();
  }

  protected static MiniKdc kdc;
  private static File kdcWorkDir;
  private static Properties kdcConf;

  @Rule
  public final Timeout testTimeout = new Timeout(10000);

  @Rule
  public TestName methodName = new TestName();

  protected static void addToTeardown(Service svc) {
    servicesToTeardown.addService(svc);
  }

  @AfterClass
  public static void teardownServices() throws IOException {
    servicesToTeardown.close();
  }


  @BeforeClass
  public static void setupKDC() throws Exception {
    // set up the KDC
    File target = new File(System.getProperty("test.dir", "target"));
    kdcWorkDir = new File(target, "kdc");
    kdcWorkDir.mkdirs();
    kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, kdcWorkDir);
    kdc.start();
  }
  
  @AfterClass
  public static void teardownKDC() throws Exception {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
  }

  public static MiniKdc getKdc() {
    return kdc;
  }

  public static File getKdcWorkDir() {
    return kdcWorkDir;
  }

  public static Properties getKdcConf() {
    return kdcConf;
  }
  
  public static File createPrincipalAndKeytab(String principal) throws Exception {
    File keytab = new File(kdcWorkDir, principal + ".keytab");
    kdc.createPrincipal(keytab, principal);
    return keytab;
  }

  public static String getPrincipalAndRealm(String principal) {
    return principal + "@" + getRealm();
  }

  protected static String getRealm() {
    return kdc.getRealm();
  }

  /**
   * Create a login context and log in as a client
   * @param principal
   * @param keytab
   * @return
   * @throws Exception
   */
  public static LoginContext login(String principal, File keytab) throws Exception {


    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));

    //client login
    Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());
    LoginContext loginContext = new LoginContext("", subject, null,
        KerberosConfiguration.createClientConfig(principal, keytab));
    loginContext.login();
    return loginContext;
  }


}

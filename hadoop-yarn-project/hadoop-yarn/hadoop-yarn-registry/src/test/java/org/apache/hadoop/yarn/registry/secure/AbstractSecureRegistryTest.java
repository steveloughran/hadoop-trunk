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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.RegistryTestHelper;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.hadoop.yarn.registry.server.services.AddingCompositeService;
import org.apache.hadoop.yarn.registry.server.services.MicroZookeeperService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
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
  public static final String ZOOKEEPER = "zookeeper/localhost";
  public static final String ALICE = "alice/localhost";
  public static final String BOB = "bob/localhost";
  public static final String SASL_AUTH_PROVIDER =
      "org.apache.hadoop.yarn.registry.secure.ExtendedSASLAuthenticationProvider";
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSecureRegistryTest.class);


  private static final AddingCompositeService servicesToTeardown =
      new AddingCompositeService("teardown");

  public static final Configuration CONF = new Configuration();

  // static initializer guarantees it is always started
  // ahead of any @BeforeClass methods
  static {
    servicesToTeardown.init(CONF);
    servicesToTeardown.start();
  }

  protected static MiniKdc kdc;
  protected static File keytab_zk;
  protected static File keytab_bob;
  protected static File keytab_alice;
  private static File kdcWorkDir;
  private static Properties kdcConf;
  protected RegistrySecurity registrySecurity = new RegistrySecurity(CONF); 
  


  @Rule
  public final Timeout testTimeout = new Timeout(10000);

  @Rule
  public TestName methodName = new TestName();
  protected MicroZookeeperService secureZK;

  /**
   * set the ZK registy parameters to bind the mini cluster to a ZK principal
   * @param conf config
   * @param principal principal
   * @param keytab keytab
   */
  protected static void bindZKPrincipal(Configuration conf,
      String principal, File keytab)  {
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_KEYTAB,
        keytab.getAbsolutePath());
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_PRINCIPAL,
        getPrincipalAndRealm(principal) );
  }

  protected static void clearZKPrincipal(Configuration conf) {
    conf.unset(RegistryConstants.KEY_REGISTRY_ZK_KEYTAB);
    conf.unset(RegistryConstants.KEY_REGISTRY_ZK_PRINCIPAL);
  }

  protected static MicroZookeeperService createSecureZKInstance(String name) throws Exception {
    Configuration conf = new Configuration();

    File testdir = new File(System.getProperty("test.dir", "target"));
    File workDir = new File(testdir, name);
    workDir.mkdirs();
    bindZKPrincipal(conf, ZOOKEEPER, keytab_zk);
    MicroZookeeperService secureZK = new MicroZookeeperService(name);
    secureZK.init(conf);
    LOG.info(secureZK.getDiagnostics());
    LOG.debug("Setting auth provider " + SASL_AUTH_PROVIDER);
    System.setProperty("zookeeper.authProvider.1", SASL_AUTH_PROVIDER);
    return secureZK;
  }

  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  protected static void addToTeardown(Service svc) {
    servicesToTeardown.addService(svc);
  }

  @AfterClass
  public static void teardownServices() throws IOException {
    servicesToTeardown.close();
  }


  @BeforeClass
  public static void setupKDCAndPrincipals() throws Exception {
    // set up the KDC
    File target = new File(System.getProperty("test.dir", "target"));
    kdcWorkDir = new File(target, "kdc");
    kdcWorkDir.mkdirs();
    kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, kdcWorkDir);
    kdc.start();

    keytab_zk = createPrincipalAndKeytab(ZOOKEEPER, "zookeeper.keytab");
    keytab_alice = createPrincipalAndKeytab(ALICE, "alice.keytab");
    keytab_bob = createPrincipalAndKeytab(BOB, "bob.keytab");
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


  public static File createPrincipalAndKeytab(String principal,
      String filename) throws Exception {
    assertNotEmpty("empty principal", principal);
    assertNotEmpty("empty host", filename);
    assertNotNull("Null KDC", kdc);
    File keytab = new File(kdcWorkDir, filename);
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
   * Log in, defaulting to the client context
   * @param principal principal
   * @param context context
   * @param keytab keytab
   * @return the logged in context
   * @throws LoginException failure to log in
   */
  protected LoginContext login(String principal,
      String context, File keytab) throws LoginException {
    String principalAndRealm = getPrincipalAndRealm(principal);
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(principal));
    Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());
    LoginContext login;
    login = new LoginContext(context, subject, null,
        KerberosConfiguration.createClientConfig(principal, keytab));
    login.login();
    return login;
  }

  @Before
  public void resetJaasConfKeys() {
    RegistrySecurity.clearJaasSystemProperties();
  }

  @Before
  public void initHadoopSecurity() {
    // resetting kerberos security
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
  }

  @After
  public void stopSecureZK() {
    ServiceOperations.stop(secureZK);
  }


  /**
   * Start the secure ZK instance using the test method name as the path
   * @throws Exception on any failure
   */
  protected void startSecureZK() throws Exception {
    secureZK = createSecureZKInstance(
        "test-" + methodName.getMethodName());
    secureZK.start();
  }

  protected void logout(LoginContext login) {
    if (login != null) {
      try {
        login.logout();
      } catch (LoginException e) {
        LOG.warn("Failure during logout: {}", e, e);

      }
    }
  }
}

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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.Shell;
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
  public static final String ZOOKEEPER = "zookeeper";
  public static final String ZOOKEEPER_LOCALHOST = "zookeeper/localhost";
  public static final String ALICE = "alice";
  public static final String ALICE_LOCALHOST = "alice/localhost";
  public static final String BOB = "bob";
  public static final String BOB_LOCALHOST = "bob/localhost";
  public static final String SASL_AUTH_PROVIDER =
      "org.apache.hadoop.yarn.registry.secure.ExtendedSASLAuthenticationProvider";
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSecureRegistryTest.class);

  public static final Configuration CONF = new Configuration();

  private static final AddingCompositeService classTeardown =
      new AddingCompositeService("classTeardown");

  // static initializer guarantees it is always started
  // ahead of any @BeforeClass methods
  static {
    classTeardown.init(CONF);
    classTeardown.start();
  }


  private final AddingCompositeService teardown =
      new AddingCompositeService("teardown");
  
  protected static MiniKdc kdc;
  protected static File keytab_zk;
  protected static File keytab_bob;
  protected static File keytab_alice;
  protected static File kdcWorkDir;
  protected static Properties kdcConf;
  protected static final RegistrySecurity registrySecurity =
      new RegistrySecurity(CONF); 
  


  @Rule
  public final Timeout testTimeout = new Timeout(900000);

  @Rule
  public TestName methodName = new TestName();
  protected MicroZookeeperService secureZK;
  protected static File jaasFile;

  /**
   * Create a secure instance
   * @param name
   * @return
   * @throws Exception
   */
  protected static MicroZookeeperService createSecureZKInstance(String name)
      throws Exception {
    String context = ZOOKEEPER;
    Configuration conf = new Configuration();

    File testdir = new File(System.getProperty("test.dir", "target"));
    File workDir = new File(testdir, name);
    workDir.mkdirs();
    System.setProperty(
        RegistryConstants.ZK_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE,
        "false");
    RegistrySecurity.validateContext(context);
    conf.set(RegistryConstants.KEY_ZKSERVICE_JAAS_CONTEXT, context);
    MicroZookeeperService secureZK = new MicroZookeeperService(name);
    secureZK.init(conf);
    LOG.info(secureZK.getDiagnostics());
    return secureZK;
  }

  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  @AfterClass
  public static void teardownClassServices() throws IOException {
    classTeardown.close();
  }

  protected static void addToClassTeardown(Service svc) {
    classTeardown.addService(svc);
  }

  protected void addToTeardown(Service svc) {
    teardown.addService(svc);
  }

  @After
  public void teardown() throws IOException {
    teardown.close();
  }
  /**
   * Sets up the KDC and a set of principals in the JAAS file
   * 
   * @throws Exception
   */
  @BeforeClass
  public static void setupKDCAndPrincipals() throws Exception {
    // set up the KDC
    System.setProperty("sun.security.krb5.debug", "true");
    File target = new File(System.getProperty("test.dir", "target"));
    kdcWorkDir = new File(target, "kdc");
    kdcWorkDir.mkdirs();
    kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, kdcWorkDir);
    kdc.start();

    keytab_zk = createPrincipalAndKeytab(ZOOKEEPER_LOCALHOST, "zookeeper.keytab");
    keytab_alice = createPrincipalAndKeytab(ALICE_LOCALHOST, "alice.keytab");
    keytab_bob = createPrincipalAndKeytab(BOB_LOCALHOST , "bob.keytab");

    StringBuilder jaas = new StringBuilder(1024);
    jaas.append(registrySecurity.createJAASEntry(ZOOKEEPER,
        ZOOKEEPER_LOCALHOST, keytab_zk));
    jaas.append(registrySecurity.createJAASEntry(ALICE, 
        ALICE_LOCALHOST , keytab_alice));
    jaas.append(registrySecurity.createJAASEntry(BOB,
        BOB_LOCALHOST, keytab_bob));

    jaasFile = new File(kdcWorkDir, "jaas.txt");
    FileUtils.write(jaasFile, jaas.toString());
    registrySecurity.bindJVMtoJAASFile(jaasFile);
    LOG.info(jaas.toString());
  }

  /**
   * For unknown reasons, the before-class setting of the JVM properties were
   * not being picked up. This method addresses that by setting them
   * before every test case
   */
  @Before
  public void forceSetTheJAASBinding() {
    System.setProperty("sun.security.krb5.debug", "true");
    registrySecurity.bindJVMtoJAASFile(jaasFile);
  }

  /**
   * Turn Kerberos Debugging on
   */
  @Before

  public void enableKerberosDebug() {
    System.setProperty("sun.security.krb5.debug", "true");
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
   * Start the secure ZK instance using the test method name as the path.
   * As the entry is saved to the {@link #secureZK} field, it
   * is automatically stopped after the test case
   * @throws Exception on any failure
   */
  protected void startSecureZK() throws Exception {
    secureZK = createSecureZKInstance("test-" + methodName.getMethodName());
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

  protected void logLoginDetails(String name,
      LoginContext loginContext) {
    Subject subject = loginContext.getSubject();
    LOG.info("Logged in as {}:\n {}" , name, subject);
  }

  /**
   * Exec ktutil and list things. This may not be on the classpath
   * @param keytab
   * @throws IOException
   */
  protected void ktList(File keytab) throws IOException {
    // ktutil --keytab=target/kdc/zookeeper.keytab list --keys

    if (!Shell.WINDOWS) {
      String path = keytab.getAbsolutePath();
      String out = Shell.execCommand(
          "ktutil",
          "--keytab=" + path,
          "list",
          "--keys"
      );
      LOG.info("Listing of keytab {}:\n{}\n", path, out);
    }
  }
} 

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
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.yarn.registry.client.services.zk.CuratorService;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.hadoop.yarn.registry.server.services.MicroZookeeperService;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

/**
 * Verify that the Mini ZK service can be started up securely
 */
public class TestSecureZKService extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureZKService.class);
  public static final String ZOOKEEPER = "zookeeper";
  private MicroZookeeperService secureZK;
  private static File keytab;
  private LoginContext loginContext;

  @BeforeClass
  public static void createPrincipal() throws Exception {
    keytab = createPrincipalAndKeytab(ZOOKEEPER);
  }
  
  
  @Before
  public void resetJaasConfKeys() {
    RegistrySecurity.resetJaasSystemProperties();
  }
  
  @After
  public void stopSecureZK() {
    ServiceOperations.stop(secureZK);
  }
  
  @After
  public void logout() throws LoginException {
    if (loginContext != null) {
      loginContext.logout();
    }
  }

  
  @Test
  public void testHasRealm() throws Throwable {
    assertNotNull(getRealm());
    LOG.info("ZK principal = {}", getPrincipalAndRealm(ZOOKEEPER));
    
  }

  
  
  @Test
  public void testCreateSecureZK() throws Throwable {
    startSecureZK();
    secureZK.stop();
  }

  @Test
  public void testInsecureClientToZK() throws Throwable {
    startSecureZK();
    CuratorService curatorService = new CuratorService("client", secureZK);
    curatorService.init(new Configuration());
    curatorService.start();
    curatorService.zkMkPath("", CreateMode.PERSISTENT);
    curatorService.zkList("/");
  }

  protected void startSecureZK() throws Exception {
    secureZK = createSecureZKInstance(
        "test-" + methodName.getMethodName());
    secureZK.start();
  }

  @Test
  public void testAuthedClientToZKNoCredentials() throws Throwable {
    startSecureZK();
    CuratorService curatorService = new CuratorService("client", secureZK);
    curatorService.init(secureZK.getConfig());
    curatorService.start();
    try {
      curatorService.zkMkPath("", CreateMode.PERSISTENT);
      fail("expected to be unauthenticated, but was allowed write access");
    } catch (AuthenticationFailedException expected) {

    }
  }

  /**
   * give the client credentials
   * @throws Throwable
   */
  @Test
  public void testAuthedSecureClientToZK() throws Throwable {
    startSecureZK();
    resetJaasConfKeys();
    loginAsClient("");

    // need to pass the keytab details down

//    kdc.
    RegistrySecurity security = new RegistrySecurity(new Configuration());
    security.setZKSaslClientProperties(null);
    Configuration clientConf = secureZK.getConfig();
    //new root per test, avoids conflict
    clientConf.set(KEY_REGISTRY_ZK_ROOT, methodName.getMethodName());

    LOG.info(" ============= Starting Curator ==========");
    CuratorService curatorService = new CuratorService("client", secureZK);
    curatorService.init(clientConf);
    curatorService.start();
    LOG.info("Curator Binding {}", curatorService.bindingDiagnosticDetails());
    curatorService.zkMkPath("", CreateMode.PERSISTENT);

    curatorService.zkList("/");
  }

  @Test
  public void testClientLogin() throws Throwable {
    loginAsClient("");
  }

  protected void loginAsClient(String name) throws LoginException {
    assertNull("already logged in", loginContext);
    loginContext = null;
    String principalAndRealm = getPrincipalAndRealm(ZOOKEEPER);
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(ZOOKEEPER));
    Subject subject = new Subject(false, principals, new HashSet<Object>(),
        new HashSet<Object>());
    loginContext = new LoginContext(name, subject, null,
        KerberosConfiguration.createClientConfig(ZOOKEEPER, keytab));
    loginContext.login();
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
        KerberosConfiguration.createServerConfig(ZOOKEEPER, keytab));
    loginContext.login();
  }
  
  
  protected static MicroZookeeperService createSecureZKInstance(String name) throws Exception {
    YarnConfiguration conf = new YarnConfiguration();

    File testdir = new File(System.getProperty("test.dir", "target"));
    File workDir = new File(testdir, name);
    workDir.mkdirs();
    bindPrincipal(conf);
    MicroZookeeperService secureZK = new MicroZookeeperService("secure");
    secureZK.init(conf);
    LOG.info(secureZK.getDiagnostics());
    return secureZK;
  }

  private static void bindPrincipal(YarnConfiguration conf) throws Exception {
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_KEYTAB, keytab.getAbsolutePath());
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_PRINCIPAL,
        getPrincipalAndRealm(ZOOKEEPER) );
  }


}

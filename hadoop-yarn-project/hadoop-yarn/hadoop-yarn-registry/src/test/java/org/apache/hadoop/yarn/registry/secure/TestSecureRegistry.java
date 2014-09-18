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
import org.apache.hadoop.fs.PathAccessDeniedException;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.yarn.registry.client.services.zk.CuratorService;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginContext;
import java.util.List;

/**
 * Verify that the Mini ZK service can be started up securely
 */
public class TestSecureRegistry extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureRegistry.class);

  @Before
  public void beforeTestSecureZKService() throws Throwable {
      enableKerberosDebugging();
//    System.setProperty(ZookeeperConfigOptions.ZK_ENABLE_SASL_CLIENT, "true");
  }
  
  @After
  public void afterTestSecureZKService() throws Throwable {
    disableKerberosDebugging();
    RegistrySecurity.clearZKSaslProperties();
  }
  
  @Test
  public void testCreateSecureZK() throws Throwable {
    startSecureZK();
    secureZK.stop();
  }

  @Test
  public void testInsecureClientToZK() throws Throwable {
    startSecureZK();
    userZookeeperToCreateRoot();
    RegistrySecurity.clearZKSaslProperties();
    
    CuratorService curatorService =
        startCuratorServiceInstance("insecure client", false);

    curatorService.zkList("/");
    curatorService.zkMkPath("", CreateMode.PERSISTENT, false,
        RegistrySecurity.WorldReadWriteACL);
  }

//  @Test
  public void testAuthedClientToZKNoCredentials() throws Throwable {
    startSecureZK();
    userZookeeperToCreateRoot();
    RegistrySecurity.clearZKSaslProperties();
    registrySecurity.logCurrentHadoopUser();
    CuratorService curatorService =
        startCuratorServiceInstance("authed with no credentials", true);
    LOG.info("Started curator client {}", curatorService);
    // read only operations MUST work
    curatorService.zkStat("");
    curatorService.zkStat("");
    try {
      curatorService.zkMkPath("", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
      fail("expected to be unauthenticated, but was allowed write access" +
           " with binding " + curatorService);
    } catch (AuthenticationFailedException expected) {
    // expected
    }
  }

  /**
   * test that ZK can write as itself
   * @throws Throwable
   */
  @Test
  public void testZookeeperCanWrite() throws Throwable {

    System.setProperty("curator-log-events", "true");
    startSecureZK();
    CuratorService curator = null;
    LoginContext login = login(ZOOKEEPER_LOCALHOST, ZOOKEEPER, keytab_zk);
    try {
      logLoginDetails(ZOOKEEPER, login);
      RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER, ZOOKEEPER);
      curator = startCuratorServiceInstance("ZK", true);
      LOG.info(curator.toString());

      addToTeardown(curator);
      curator.zkMkPath("/", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
      curator.zkList("/");
      curator.zkMkPath("/zookeeper", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
    } finally {
      logout(login);
      ServiceOperations.stop(curator);
    }
  }


  /**
   * give the client credentials
   * @throws Throwable
   */
//  @Test
  public void testAliceCanWrite() throws Throwable {

    System.setProperty("curator-log-events", "true");
    startSecureZK();
    userZookeeperToCreateRoot();
    RegistrySecurity.clearZKSaslProperties();
    LoginContext aliceLogin = login(ALICE_LOCALHOST, ALICE, keytab_alice);
    try {
      logLoginDetails(ALICE, aliceLogin);
      ktList(keytab_alice);
      RegistrySecurity.setZKSaslClientProperties(ALICE, ALICE);
      describe(LOG, "Starting Alice Curator");
      CuratorService alice =
          startCuratorServiceInstance("alice's", true);
      LOG.info(alice.toString());

      addToTeardown(alice);
      
      // stat must work
      alice.zkStat("");

      alice.zkList("/");
      alice.zkMkPath("/alice", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
    } finally {
      logout(aliceLogin);
    }

  }


//  @Test
  public void testAliceCanWriteButNotBob() throws Throwable {
    startSecureZK();
    // alice
    CuratorService alice = null;
    LoginContext aliceLogin =
        login(ALICE_LOCALHOST, ALICE, keytab_alice);
    try {
      alice = startCuratorServiceInstance("alice's", true);
      alice.zkList("/");
      alice.zkMkPath("/alice", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
      Stat stat = alice.zkStat("/alice");
      LOG.info("stat /alice = {}", stat);
      List<ACL> acls = alice.zkGetACLS("/alice");
      LOG.info(RegistrySecurity.aclsToString(acls));
    } finally {
      ServiceOperations.stop(alice);
      aliceLogin.logout();
    }
    CuratorService bobCurator = null;
    LoginContext bobLogin =
        login(BOB_LOCALHOST, BOB, keytab_bob);

    try {
      bobCurator = startCuratorServiceInstance("bob's", true);
      bobCurator.zkMkPath("/alice/bob", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
      fail("Expected a failure —but bob could create a path under /alice");
      bobCurator.zkDelete("/alice", false, null);
    } catch (PathAccessDeniedException expected) {
      // expected
    } finally {
      ServiceOperations.stop(bobCurator);
      bobLogin.logout();
    }


  }


  protected CuratorService startCuratorServiceInstance(String name,
      boolean secure) {
    Configuration clientConf = new Configuration();
    clientConf.set(KEY_REGISTRY_ZK_ROOT, "/");
    clientConf.setBoolean(KEY_REGISTRY_SECURE, secure);
    describe(LOG, "Starting Curator service");
    CuratorService curatorService = new CuratorService(name, secureZK);
    curatorService.init(clientConf);
    curatorService.start();
    LOG.info("Curator Binding {}",
        curatorService.bindingDiagnosticDetails());
    return curatorService;
  }

  /**
   * have the ZK user create the root dir.
   * This logs out the ZK user after and stops its curator instance,
   * to avoid contamination
   * @throws Throwable
   */
  public void userZookeeperToCreateRoot() throws Throwable {

    System.setProperty("curator-log-events", "true");
    CuratorService curator = null;
    LoginContext login = login(ZOOKEEPER_LOCALHOST, ZOOKEEPER, keytab_zk);
    try {
      logLoginDetails(ZOOKEEPER, login);
      RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER, ZOOKEEPER);
      curator = startCuratorServiceInstance("ZK", true);
      LOG.info(curator.toString());

      addToTeardown(curator);
      curator.zkMkPath("/", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
    } finally {
      logout(login);
      ServiceOperations.stop(curator);
    }
  }
}

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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.server.services.RMRegistryOperationsService;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.KEY_REGISTRY_CLIENT_AUTH;
import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.KEY_REGISTRY_CLIENT_JAAS_CONTEXT;
import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.KEY_REGISTRY_SECURE;
import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.KEY_REGISTRY_SYSTEM_ACCOUNTS;
import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.REGISTRY_CLIENT_AUTH_KERBEROS;

/**
 * Verify that the {@link RMRegistryOperationsService} works securely
 */
public class TestSecureRMRegistryOperations extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureRMRegistryOperations.class);
  private Configuration secureConf;
  private Configuration zkClientConf;
  private UserGroupInformation zookeeperUGI;


  @Before
  public void setupTestSecureRMRegistryOperations() throws Exception {
    startSecureZK();
    secureConf = new Configuration();
    secureConf.setBoolean(KEY_REGISTRY_SECURE, true);

    // create client conf containing the ZK quorum
    zkClientConf = new Configuration(secureZK.getConfig());
    assertNotEmpty(zkClientConf.get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM));
    
    // ZK is in charge
    secureConf.set(KEY_REGISTRY_SYSTEM_ACCOUNTS, "sasl:zookeeper@");
    zookeeperUGI = loginUGI(ZOOKEEPER, keytab_zk);
  }

  @After
  public void teardownTestSecureRMRegistryOperations() {
  }

  /**
   * Create the RM registry operations as the current user
   * @return the service
   * @throws LoginException
   * @throws FileNotFoundException
   */
  public RMRegistryOperationsService startRMRegistryOperations() throws
      LoginException, IOException, InterruptedException {
    ktListRobust(keytab_zk);
/*
    RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER_REALM,
        ZOOKEEPER);
*/
    // kerberos
    secureConf.set(KEY_REGISTRY_CLIENT_AUTH,
        REGISTRY_CLIENT_AUTH_KERBEROS);
    secureConf.set(KEY_REGISTRY_CLIENT_JAAS_CONTEXT, ZOOKEEPER_CLIENT_CONTEXT);

    RMRegistryOperationsService registryOperations = zookeeperUGI.doAs(
        new PrivilegedExceptionAction<RMRegistryOperationsService>() {
          @Override
          public RMRegistryOperationsService run() throws Exception {
            RMRegistryOperationsService operations
                = new RMRegistryOperationsService("rmregistry", secureZK);
            addToTeardown(operations);
            operations.init(secureConf);
            LOG.info(operations.bindingDiagnosticDetails());
            operations.start();
            return operations;
          }
        }
    );


    return registryOperations;
  }
  
/*

  @Test
  public void testInsecureClientToZK() throws Throwable {

    userZookeeperToCreateRoot();
    RegistrySecurity.clearZKSaslProperties();
    
    CuratorService curatorService =
        startCuratorServiceInstance("insecure client", false);

    curatorService.zkList("/");
    curatorService.zkMkPath("", CreateMode.PERSISTENT, false,
        RegistrySecurity.WorldReadWriteACL);
  }
*/

  /**
   * test that ZK can write as itself
   * @throws Throwable
   */
  @Test
  public void testZookeeperCanWriteUnderSystem() throws Throwable {

    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    RegistryOperations operations = rmRegistryOperations;
    operations.mknode(RegistryConstants.PATH_SYSTEM_SERVICES + "hdfs",
        false);
  }


  @Test
  public void testAnonReadAccess() throws Throwable {
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();

    RegistryOperations operations =
        RegistryOperationsFactory.createAnonymousInstance(zkClientConf);
    addToTeardown(operations);
    operations.start();
   
    assertFalse("RegistrySecurity.isClientSASLEnabled()==true",
        RegistrySecurity.isClientSASLEnabled());
    assertFalse("ZooKeeperSaslClient.isEnabled()==true",
        ZooKeeperSaslClient.isEnabled());
    RegistryPathStatus[] stats =
        operations.list(RegistryConstants.PATH_SYSTEM_SERVICES);
  }
  
  @Test
  public void testAnonNoWriteAccess() throws Throwable {
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();

    RegistryOperations operations =
        RegistryOperationsFactory.createAnonymousInstance(zkClientConf);
    addToTeardown(operations);
    describe(LOG, "starting anon operations");
    operations.start();
    
    try {
      String servicePath = RegistryConstants.PATH_SYSTEM_SERVICES + "hdfs";
      operations.mknode(servicePath,
          false);
      fail("should have failed to create a node under " + servicePath);
    } catch (PathAccessDeniedException expected) {
      // expected
    }
  }


  @Test
  public void testCreateAlicePath() throws Throwable {
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    String aliceHome = rmRegistryOperations.initUserRegistry(ALICE);
    
  }
  
  /**
   * give the client credentials
   * @throws Throwable
   */
//  @Test
/*  public void testAliceCanWrite() throws Throwable {

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

  }*/


}

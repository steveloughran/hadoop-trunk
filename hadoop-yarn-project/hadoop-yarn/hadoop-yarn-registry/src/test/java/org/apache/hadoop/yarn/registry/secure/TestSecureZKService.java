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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.yarn.registry.client.services.zk.CuratorService;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

/**
 * Verify that the Mini ZK service can be started up securely
 */
public class TestSecureZKService extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureZKService.class);


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

  @Test
  public void testAuthedClientToZKNoCredentials() throws Throwable {
    startSecureZK();
    RegistrySecurity.clearJaasSystemProperties();
    RegistrySecurity.clearZKSaslProperties();
    registrySecurity.logCurrentUser();
    
    CuratorService curatorService = new CuratorService("client", secureZK);
    Configuration config = new Configuration();
    curatorService.init(config);
    curatorService.start();
    LOG.info("Started curator client {}", curatorService);
    try {
      curatorService.zkMkPath("", CreateMode.PERSISTENT);
      fail("expected to be unauthenticated, but was allowed write access" +
           " with binding " + curatorService);
    } catch (AuthenticationFailedException expected) {
    // expected
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


    // alice
    RegistrySecurity.setZKSaslClientProperties(null);
    CuratorService alice =
        startCuratorServiceInstance(ALICE, keytab_alice);
    addToTeardown(alice);
    alice.zkList("/");

  }

  protected CuratorService startCuratorServiceInstance(String principal,
      File keytab) {
    Configuration clientConf = new Configuration();
    bindZKPrincipal(clientConf, principal, keytab);
    clientConf.set(KEY_REGISTRY_ZK_ROOT, "/");
    describe(LOG, "Starting Curator service");
    CuratorService curatorService = new CuratorService("client", secureZK);
    curatorService.init(clientConf);
    curatorService.start();
    LOG.info("Curator Binding {}",
        curatorService.bindingDiagnosticDetails());
    return curatorService;
  }


  @Test
  public void testAliceCanWriteButNotBob() throws Throwable {
    startSecureZK();
    // alice
    CuratorService alice =
        startCuratorServiceInstance(ALICE, keytab_alice);
    addToTeardown(alice);
    alice.zkList("/");
    alice.zkMkPath("/alice", CreateMode.PERSISTENT, false,
        RegistrySecurity.WorldReadOwnerWriteACL);
    Stat stat = alice.zkStat("/alice");
    LOG.info("stat /alice = {}", stat);
    List<ACL> acls = alice.zkGetACLS("/alice");
    registrySecurity.logACLs(acls);

    CuratorService bobCurator =
        startCuratorServiceInstance(BOB, keytab_bob);
    addToTeardown(bobCurator);
    try {
      bobCurator.zkMkPath("/alice/bob", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadOwnerWriteACL);
      fail("Expected a failure —but bob could create a path under /alice");
      bobCurator.zkDelete("/alice", false, null);
    } catch (AuthenticationFailedException expected) {
      // expected
    }


  }

}

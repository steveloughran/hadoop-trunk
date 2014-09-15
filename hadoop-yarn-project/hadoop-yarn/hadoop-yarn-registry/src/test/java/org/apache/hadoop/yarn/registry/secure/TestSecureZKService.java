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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.security.PrivilegedExceptionAction;

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
    LoginContext login = loginAsClient(ALICE, "", keytab_alice);

    RegistrySecurity.setZKSaslClientProperties(null);
    final Configuration clientConf = secureZK.getConfig();
    // sets root to /
    clientConf.set(KEY_REGISTRY_ZK_ROOT, "/");
    Subject subject = loginContext.getSubject();
    UserGroupInformation ugi =
        UserGroupInformation.getUGIFromSubject(subject);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        describe(LOG, "Starting Curator service");
        CuratorService curatorService = new CuratorService("client", secureZK);
        try {
          curatorService.init(clientConf);
          curatorService.start();
          LOG.info("Curator Binding {}",
              curatorService.bindingDiagnosticDetails());
          curatorService.zkList("/");
        } finally {
          describe(LOG, "shutdown curator");
          ServiceOperations.stop(curatorService);
        }
        return null;
      }
    });

  }

}

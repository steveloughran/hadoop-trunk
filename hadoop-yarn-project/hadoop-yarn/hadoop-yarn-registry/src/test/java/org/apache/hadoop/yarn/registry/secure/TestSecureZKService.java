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


import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.services.zk.CuratorService;
import org.apache.hadoop.yarn.registry.server.services.MicroZookeeperService;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Verify that the Mini ZK service can be started up securely
 */
public class TestSecureZKService extends AbstractSecureRegistryTest{
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureZKService.class);
  public static final String ZOOKEEPER = "zookeeper";
  private MicroZookeeperService secureZK;
  private static File keytab;

  @BeforeClass
  public static void createPrincipal() throws Exception {
    keytab = createPrincipalAndKeytab(ZOOKEEPER);
  }
  
  @After
  public void stopSecureZK() {
    ServiceOperations.stop(secureZK);
  }

  @Test
  public void testHasRealm() throws Throwable {
    assertNotNull(getRealm());
    LOG.info("ZK principal = {}", getPrincipalAndRealm(ZOOKEEPER));
    
  }

  
  
  @Test
  public void testCreateSecureZK() throws Throwable {
    secureZK = createSecureZKInstance(
        "test-" + methodName.getMethodName());
    secureZK.start();
    secureZK.stop();
  }

  @Test
  public void testInsecureClientToZK() throws Throwable {
    secureZK = createSecureZKInstance(
        "test-" + methodName.getMethodName());
    secureZK.start();
    secureZK.stop();
    secureZK.supplyBindingInformation();
    CuratorService curatorService = new CuratorService("client", secureZK);
    curatorService.init(secureZK.getConfig());
    curatorService.start();

    curatorService.zkList("/");
  }
  
  protected static MicroZookeeperService createSecureZKInstance(String name) throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    MicroZookeeperService secureZK = new MicroZookeeperService("secure");
    secureZK.init(conf);
    File testdir = new File(System.getProperty("test.dir", "target"));
    File workDir = new File(testdir, name);
    workDir.mkdirs();
    bindPrincipal(conf);
    secureZK.setupSecurity(conf);
    return secureZK;
  }

  private static void bindPrincipal(YarnConfiguration conf) throws Exception {
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_KEYTAB, keytab.getAbsolutePath());
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_PRINCIPAL,
        getPrincipalAndRealm(ZOOKEEPER) );
  }


}

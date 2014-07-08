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

package org.apache.hadoop.yarn.registry.server.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.AbstractZKRegistryTest;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.junit.Test;

public class TestRegistryZKService extends AbstractZKRegistryTest {


  @Test
  public void testRegistryStart() throws Throwable {
    RegistryZKService service = new RegistryZKService("registry");
    service.init(createRegistryConfiguration());
    service.start();
  }

  protected YarnConfiguration createRegistryConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(RegistryConstants.ZK_CONNECTION_TIMEOUT, 1000);
    conf.setInt(RegistryConstants.ZK_RETRY_INTERVAL, 500);
    conf.setInt(RegistryConstants.ZK_RETRY_TIMES, 10);
    conf.setInt(RegistryConstants.ZK_RETRY_CEILING, 10);
    conf.set(RegistryConstants.ZK_HOSTS, zookeeper.getConnectionString());
    return conf;
  }
}

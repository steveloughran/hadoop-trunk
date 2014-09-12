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

package org.apache.hadoop.yarn.registry;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.services.RMRegistryOperationsService;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.*;

public class AbstractRegistryTest extends AbstractZKRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRegistryTest.class);
  protected RMRegistryOperationsService registry;
  protected RegistryOperations operations;
  


  @Before
  public void setupRegistry() throws IOException {
    registry = new RMRegistryOperationsService("yarnRegistry");
    registry.init(createRegistryConfiguration());
    registry.start();
    registry.createRegistryPaths();
    addToTeardown(registry);
    operations = registry;
    operations.delete(ENTRY_PATH, true);
  }


  /**
   * Create a service entry with the sample endpoints, and put it
   * at the destination
   * @param path path
   * @param createFlags flags
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord putExampleServiceEntry(String path, int createFlags) throws
      IOException,
      URISyntaxException {
    return putExampleServiceEntry(path, createFlags, PersistencePolicies.PERMANENT);
  }

  /**
   * Create a service entry with the sample endpoints, and put it
   * at the destination
   * @param path path
   * @param createFlags flags
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord putExampleServiceEntry(String path,
      int createFlags,
      int persistence)
      throws IOException, URISyntaxException {
    ServiceRecord record = buildExampleServiceEntry(persistence);

    registry.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, record, createFlags);
    return record;
  }

  /**
   * Assert a path exists
   * @param path
   * @throws IOException
   */
  public void assertPathExists(String path) throws IOException {
    operations.stat(path);
  }

  public void assertPathNotFound(String path) throws IOException {
    try {
      operations.stat(path);
      fail("Path unexpectedly found: " + path);
    } catch (PathNotFoundException e) {

    }
  }

  public void assertResolves(String path) throws IOException {
    operations.resolve(path);
  }

}

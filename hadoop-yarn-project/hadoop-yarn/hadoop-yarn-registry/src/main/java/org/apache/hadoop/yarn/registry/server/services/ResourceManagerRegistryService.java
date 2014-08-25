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
import org.apache.hadoop.yarn.registry.client.binding.BindingUtils;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;

/**
 * Extends the registry operations with extra support for resource management
 * operations, in particular setting up paths with the correct security
 * permissions.
 */
public class ResourceManagerRegistryService extends RegistryOperationsService {

  private List<ACL> rootRegistryACL;

  private List<ACL> userAcl;

  public ResourceManagerRegistryService() {
    super("ResourceManagerRegistryService");
  }

  public ResourceManagerRegistryService(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    userAcl = parseACLs(PERMISSIONS_REGISTRY_USERS);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    // create the root directories
    List<ACL> rootACL = getACLs(REGISTRY_ZK_ACL, PERMISSIONS_REGISTRY_ROOT);
    maybeCreate("", CreateMode.PERSISTENT, rootACL, false);
    maybeCreate(PATH_USERS, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_USERS), false);
    maybeCreate(PATH_SYSTEM_SERVICES_PATH, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_SYSTEM), false);

  }


  /**
   * Create the initial registry paths
   * @throws IOException
   */
  public void createRegistryPaths() throws IOException {
    // create the root directories
    rootRegistryACL = getACLs(REGISTRY_ZK_ACL, PERMISSIONS_REGISTRY_ROOT);
    maybeCreate("", CreateMode.PERSISTENT, rootRegistryACL, false);

    maybeCreate(PATH_USERS, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_USERS), false);
    maybeCreate(PATH_SYSTEM_SERVICES_PATH, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_SYSTEM), false);
  }

  /**
   * Create the path for a user
   * @param username
   * @throws IOException
   */
  public void createUserPath(String username) throws IOException {
    String path = BindingUtils.userPath(username);
    maybeCreate(path, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_USER), false);
  }


  private List<ACL> createAclForUser(String username) throws IOException {
    return parseACLs(PERMISSIONS_REGISTRY_USER);
  }

}

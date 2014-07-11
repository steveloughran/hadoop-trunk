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

package org.apache.hadoop.yarn.registry.client.binding.zk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.registry.client.api.RegistryWriter;
import org.apache.hadoop.yarn.registry.client.binding.AbstractRegistryWriterService;
import org.apache.hadoop.yarn.registry.client.binding.JsonMarshal;
import org.apache.hadoop.yarn.registry.client.types.ComponentEntry;
import org.apache.hadoop.yarn.registry.client.types.ServiceEntry;
import org.apache.hadoop.yarn.registry.server.services.RegistryZKService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.registry.client.binding.BindingUtils.*;
import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The ZK client is R/W and is used  to register
 * services as well as query them.
 */
public class ZookeeperRegistryClient extends RegistryZKService
    implements RegistryWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryZKService.class);

  private final JsonMarshal.ServiceEntryMarshal serviceEntryMarshal 
      = new JsonMarshal.ServiceEntryMarshal();
  private final JsonMarshal.ComponentEntryMarshal componentEntryMarshal 
      = new JsonMarshal.ComponentEntryMarshal();

  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USER = "world:anyone:rwcda";
  
  public ZookeeperRegistryClient(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // create the root directories
    if (maybeCreate(SYSTEM_PATH, CreateMode.PERSISTENT)) {
      LOG.info("Created ");
    }
    maybeCreate(USERS_PATH, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_USERS));
    maybeCreate(SYSTEM_PATH, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_SYSTEM));
  }

  /**
   * Create the ACL needed to grant the user access to their part of the tree
   * @param user user name
   * @return an ACL list
   * @throws IOException
   */
  private List<ACL> fullUserAccess(String user) throws IOException {
    return parseACLs(PERMISSIONS_REGISTRY_USER);
  }
  
  @Override
  public void putServiceEntry(String user,
      String serviceClass,
      String serviceName,
      ServiceEntry entry) throws IOException {
    byte[] bytes = serviceEntryMarshal.toBytes(entry);
    maybeCreateServiceClassPath(user, serviceClass);
    String path = buildServicePath(user, serviceClass, serviceName);
    set(path, CreateMode.PERSISTENT, bytes, fullUserAccess(user));
  }

  void maybeCreateUserPath(String user) throws IOException {
    maybeCreate(buildUserPath(user),
        CreateMode.PERSISTENT,
        fullUserAccess(user));
  }

  void maybeCreateServiceClassPath(String user, String serviceClass) throws IOException {
    maybeCreateUserPath(user);
    maybeCreate(buildServiceClassPath(user, serviceClass),
        CreateMode.PERSISTENT,
        fullUserAccess(user));
  }

  @Override
  public void deleteServiceEntry(String user,
      String serviceClass,
      String serviceName) throws IOException {
    rm(buildServicePath(user,serviceClass, serviceName), true);
  }

  /**
   * Components can only be created if the service is present
   * @param user
   * @param serviceClass
   * @param serviceName
   * @param componentName
   * @param entry
   * @param ephemeral
   * @throws IOException
   */
  @Override
  public void putComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName,
      ComponentEntry entry,
      boolean ephemeral) throws IOException {

    verifyExists(buildServicePath(user, serviceClass, serviceName));
    String componentPath =
        buildComponentPath(user, serviceClass, serviceName, componentName);
    set(componentPath,
        ephemeral? CreateMode.EPHEMERAL: CreateMode.PERSISTENT,
        componentEntryMarshal.toBytes(entry),
        fullUserAccess(user));
  }

  @Override
  public void deleteComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    rm(buildComponentPath(user, serviceClass, serviceName, componentName),
        false);
  }

  @Override
  public List<String> listServiceClasses(String user) throws IOException {
    return listChildren(buildUserPath(user));
  }

  @Override
  public boolean serviceClassExists(String user, String serviceClass) throws
      IOException {
    return exists(buildServiceClassPath(user, serviceClass));
  }

  @Override
  public List<String> listServices(String user, String serviceClass) throws
      IOException {
    return listChildren(buildServiceClassPath(user, serviceClass));
  }

  @Override
  public boolean serviceExists(String user,
      String serviceClass,
      String serviceName) throws IOException {
    return exists(buildServicePath(user, serviceClass, serviceName));
  }

  @Override
  public ServiceEntry getServiceInstance(String user,
      String serviceClass,
      String serviceName) throws IOException {
    String path = buildServicePath(user, serviceClass, serviceName);
    return serviceEntryMarshal.fromBytes(read(path));

  }

  @Override
  public List<String> listComponents(String user,
      String serviceClass,
      String serviceName) throws IOException {
    return listChildren(buildServicePath(user, serviceClass, serviceName));
  }

  @Override
  public ComponentEntry getComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    String path =
        buildComponentPath(user, serviceClass, serviceName, componentName);
    return componentEntryMarshal.fromBytes(read(path));
  }

  @Override
  public boolean componentExists(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    return exists(buildComponentPath(user, serviceClass, serviceName,
        componentName));

  }
}

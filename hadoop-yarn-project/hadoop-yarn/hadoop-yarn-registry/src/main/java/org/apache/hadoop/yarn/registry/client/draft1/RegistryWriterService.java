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

package org.apache.hadoop.yarn.registry.client.draft1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.JsonMarshal;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.client.services.CuratorService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.registry.client.binding.BindingUtils.componentListPath;
import static org.apache.hadoop.yarn.registry.client.binding.BindingUtils.componentPath;
import static org.apache.hadoop.yarn.registry.client.draft1.Draft1BindingUtils.livenessPath;
import static org.apache.hadoop.yarn.registry.client.binding.BindingUtils.servicePath;
import static org.apache.hadoop.yarn.registry.client.binding.BindingUtils.serviceclassPath;
import static org.apache.hadoop.yarn.registry.client.binding.BindingUtils.userPath;

/**
 * The YARN ZK registry service is R/W and is used  to register
 * services as well as query them.
 * 
 * It's a YARN service: ephemeral nodes last as long as the client exists
 */
public class RegistryWriterService extends CuratorService
    implements RegistryWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryWriterService.class);

  private final JsonMarshal.ServiceRecordMarshal serviceRecordMarshal
      = new JsonMarshal.ServiceRecordMarshal();

  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USER = "world:anyone:rwcda";
  private static byte[] NO_DATA = new byte[0];
  private List<ACL> userAcl;

  public RegistryWriterService(String name) {
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
    maybeCreate(PATH_SYSTEM_SERVICES_PATH, CreateMode.PERSISTENT);
    maybeCreate(PATH_USERS, CreateMode.PERSISTENT,
        userAcl);
    maybeCreate(PATH_SYSTEM_SERVICES_PATH, CreateMode.PERSISTENT,
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

  public List<ACL> getUserAcl() {
    return userAcl;
  }

  @Override
  public void putServiceEntry(String user,
      String serviceClass,
      String serviceName,
      ServiceRecord entry) throws IOException {
    byte[] bytes = serviceRecordMarshal.toBytes(entry);
    maybeCreateServiceClassPath(user, serviceClass);
    String servicePath = servicePath(user, serviceClass, serviceName);
    List<ACL> userAccess = fullUserAccess(user);
    if (zkSet(servicePath, CreateMode.PERSISTENT, bytes, userAccess)) {
      maybeCreate(servicePath + RegistryConstants.PATH_COMPONENTS,
          CreateMode.PERSISTENT,
          userAccess);
    }
  }

  @Override
  public void putServiceLiveness(String user,
      String serviceClass,
      String serviceName,
      boolean ephemeral, boolean forceDelete) throws IOException {
    serviceMustExist(user, serviceClass, serviceName);

    String liveness = livenessPath(user, serviceClass, serviceName);
    LOG.debug("putServiceLiveness() on {} => {}", liveness, ephemeral);
    CreateMode mode = ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
    while (true) try {
      zkCreate(liveness, mode, NO_DATA, fullUserAccess(user));
      break;
    } catch (FileAlreadyExistsException e) {
      // file exists: choose policy to react to this
      if (forceDelete) {
        zkDelete(liveness, false);
      } else {
        throw e;
      }
    }
  }

  /**
   * verify that the service exists
   * @param user
   * @param serviceClass
   * @param serviceName
   * @throws IOException
   */
  protected void serviceMustExist(String user,
      String serviceClass,
      String serviceName) throws IOException {
    zkPathMustExist(servicePath(user, serviceClass, serviceName));
  }

  @Override
  public void deleteServiceLiveness(String user,
      String serviceClass,
      String serviceName) throws IOException {
    String liveness =
        Draft1BindingUtils.livenessPath(user, serviceClass, serviceName);
    zkDelete(liveness, false);
  }

  @Override
  public boolean isServiceLive(String user,
      String serviceClass,
      String serviceName) throws IOException {
    String path =
        Draft1BindingUtils.livenessPath(user, serviceClass, serviceName);
    return zkPathExists(path);
  }

  void maybeCreateUserPath(String user) throws IOException {
    maybeCreate(userPath(user),
        CreateMode.PERSISTENT,
        fullUserAccess(user));
  }

  void maybeCreateServiceClassPath(String user, String serviceClass) throws
      IOException {
    maybeCreateUserPath(user);
    maybeCreate(serviceclassPath(user, serviceClass),
        CreateMode.PERSISTENT,
        fullUserAccess(user));
  }

  @Override
  public void deleteServiceEntry(String user,
      String serviceClass,
      String serviceName) throws IOException {
    zkDelete(servicePath(user, serviceClass, serviceName), true);
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
      ServiceRecord entry,
      boolean ephemeral) throws IOException {
    String servicePath = zkPathMustExist(
        servicePath(user, serviceClass, serviceName));
    maybeCreate(servicePath + RegistryConstants.PATH_COMPONENTS,
        CreateMode.PERSISTENT,
        fullUserAccess(user));
    String componentPath =
        componentPath(user, serviceClass, serviceName, componentName);
    zkSet(componentPath,
        ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT,
        serviceRecordMarshal.toBytes(entry),
        fullUserAccess(user));
  }

  @Override
  public void deleteComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    zkDelete(componentPath(user, serviceClass, serviceName, componentName),
        false);
  }

  @Override
  public List<String> listServiceClasses(String user) throws IOException {
    return zKListChildren(userPath(user));
  }

  @Override
  public boolean serviceClassExists(String user, String serviceClass) throws
      IOException {
    return zkPathExists(serviceclassPath(user, serviceClass));
  }

  @Override
  public List<String> listServices(String user, String serviceClass) throws
      IOException {
    return zKListChildren(serviceclassPath(user, serviceClass));
  }

  @Override
  public boolean serviceExists(String user,
      String serviceClass,
      String serviceName) throws IOException {
    return zkPathExists(servicePath(user, serviceClass, serviceName));
  }

  @Override
  public ServiceRecord getServiceInstance(String user,
      String serviceClass,
      String serviceName) throws IOException {
    String path = servicePath(user, serviceClass, serviceName);
    return serviceRecordMarshal.fromBytes(zkRead(path));
  }

  @Override
  public List<String> listComponents(String user,
      String serviceClass,
      String serviceName) throws IOException {
    return zKListChildren(componentListPath(user, serviceClass, serviceName));
  }

  @Override
  public ServiceRecord getComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    String path =
        componentPath(user, serviceClass, serviceName, componentName);
    return serviceRecordMarshal.fromBytes(zkRead(path));
  }

  @Override
  public boolean componentExists(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    return zkPathExists(componentPath(user, serviceClass, serviceName,
        componentName));
  }
}

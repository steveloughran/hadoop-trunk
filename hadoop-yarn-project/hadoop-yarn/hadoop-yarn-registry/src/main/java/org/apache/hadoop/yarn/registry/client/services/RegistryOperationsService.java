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

package org.apache.hadoop.yarn.registry.client.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.binding.JsonMarshal;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryZKUtils.*;

import org.apache.hadoop.yarn.registry.client.binding.RegistryZKUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.yarn.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.hadoop.yarn.registry.client.types.CreateFlags;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The YARN ZK registry operations service.
 *
 * It's a YARN service: ephemeral nodes last as long as the client exists
 */
public class RegistryOperationsService extends CuratorService 
implements RegistryOperations{

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryOperationsService.class);

  private final JsonMarshal.ServiceRecordMarshal serviceRecordMarshal
      = new JsonMarshal.ServiceRecordMarshal();

  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USER = "world:anyone:rwcda";
  private static byte[] NO_DATA = new byte[0];
  private List<ACL> userAcl;

  public RegistryOperationsService(String name) {
    this(name, null);
  }

  public RegistryOperationsService() {
    this("RegistryOperationsService");
  }

  public RegistryOperationsService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    userAcl = parseACLs(PERMISSIONS_REGISTRY_USERS);
  }

  public List<ACL> getUserAcl() {
    return userAcl;
  }

  protected void validatePath(String path) throws InvalidPathnameException {
    RegistryZKUtils.validateElementsAsDNS(path);
  }
  
  @Override
  public boolean mkdir(String path, boolean createParents) throws
      PathNotFoundException,
      NoChildrenForEphemeralsException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    return zkMkPath(path, CreateMode.PERSISTENT, createParents, getUserAcl());
  }

  @Override
  public void create(String path,
      ServiceRecord record,
      int createFlags) throws
      PathNotFoundException,
      NoChildrenForEphemeralsException,
      FileAlreadyExistsException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    byte[] bytes = serviceRecordMarshal.toBytes(record);

    CreateMode mode = ((createFlags & CreateFlags.EPHEMERAL) != 0)
        ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;

    zkSet(path, mode, bytes, getUserAcl(),
        ((createFlags & CreateFlags.OVERWRITE) != 0));
  }

  @Override
  public ServiceRecord resolve(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    byte[] bytes = zkRead(path);
    return serviceRecordMarshal.fromBytes(bytes, 0);
  }

  @Override
  public RegistryPathStatus stat(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    Stat stat = zkStat(path);
    RegistryPathStatus status = new RegistryPathStatus(
        path,
        stat.getCtime(),
        stat.getDataLength(),
        stat.getDataLength() != 0);
    return status;
  }

  @Override
  public RegistryPathStatus[] listDir(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    List<String> childNames = zkList(path);
    RegistryPathStatus[] results = new RegistryPathStatus[0];
    int size = childNames.size();
    ArrayList<RegistryPathStatus> childList = new ArrayList<RegistryPathStatus>(
        size);
    for (String childName : childNames) {
      childList.add(stat(join(path, childName)));
    }
    return childList.toArray(new RegistryPathStatus[size]);
  }

  @Override
  public void delete(String path, boolean recursive) throws
      PathNotFoundException,
      PathIsNotEmptyDirectoryException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    zkDelete(path, recursive);
  }
}

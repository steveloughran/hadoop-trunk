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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.binding.JsonMarshal;
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

  public RegistryOperationsService(String name) {
    super(name);
  }

  public RegistryOperationsService() {
    super("RegistryOperationsService");
  }

  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USER = "world:anyone:rwcda";
  private static byte[] NO_DATA = new byte[0];
  private List<ACL> userAcl;

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
        parseACLs(PERMISSIONS_REGISTRY_USERS));
    maybeCreate(PATH_SYSTEM_SERVICES_PATH, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_SYSTEM));
  }

  public List<ACL> getUserAcl() {
    return userAcl;
  }

  @Override
  public void mkdir(String path) throws
      PathNotFoundException,
      NoChildrenForEphemeralsException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    
    zkMkPath(path, CreateMode.PERSISTENT, getUserAcl());
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
    byte[] bytes = serviceRecordMarshal.toBytes(record);

    CreateMode mode = ((createFlags & CreateFlags.EPHEMERAL) != 0)
        ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;

    zkSet(path, mode, bytes, getUserAcl());
  }

  @Override
  public ServiceRecord resolve(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    return serviceRecordMarshal.fromBytes(zkRead(path));
  }

  @Override
  public RegistryPathStatus stat(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    RegistryPathStatus status = new RegistryPathStatus();
    status.path = path;
    Stat stat = zkStat(path);
    status.size = stat.getDataLength();
    status.hasRecord = status.size != 0;
    status.time = stat.getCtime();
    return null;
  }

  @Override
  public RegistryPathStatus[] listDir(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    List<String> childNames = zkList(path);
    RegistryPathStatus[] results = new RegistryPathStatus[0];
    int size = childNames.size();
    ArrayList<RegistryPathStatus> childList = new ArrayList<RegistryPathStatus>(
        size);
    for (String childName : childNames) {
      childList.add(stat(path + "/" + childName));
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
    zkDelete(path, recursive);

  }
}

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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils.*;

import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.yarn.registry.client.api.CreateFlags;
import org.apache.hadoop.yarn.registry.client.services.zk.CuratorService;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
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
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryOperationsService extends CuratorService 
  implements RegistryOperations{

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryOperationsService.class);

  private final RecordOperations.ServiceRecordMarshal serviceRecordMarshal
      = new RecordOperations.ServiceRecordMarshal();

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
    userAcl = RegistrySecurity.WorldReadOwnerWriteACL;
  }

  public List<ACL> getUserAcl() {
    return userAcl;
  }

  protected void validatePath(String path) throws InvalidPathnameException {
    RegistryPathUtils.validateElementsAsDNS(path);
  }
  
  @Override
  public boolean mkdir(String path, boolean createParents) throws
      PathNotFoundException,
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
      FileAlreadyExistsException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    Preconditions.checkArgument(record != null, "null record");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(record.id), 
        "empty record ID");
    validatePath(path);
    LOG.info("Registered at {} : {}", path, record);

    CreateMode mode = CreateMode.PERSISTENT;
    byte[] bytes = serviceRecordMarshal.toByteswithHeader(record);
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
    return serviceRecordMarshal.fromBytesWithHeader(path, bytes);
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
        stat.getNumChildren());
    LOG.debug("Stat {} => {}", path, status);
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
    zkDelete(path, recursive, null);
  }
}

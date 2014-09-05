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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.registry.client.binding.BindingUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.services.RegistryBindingSource;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Extends the registry operations with extra support for resource management
 * operations, in particular setting up paths with the correct security
 * permissions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ResourceManagerRegistryService extends RegistryOperationsService {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceManagerRegistryService.class);
  
  private List<ACL> rootRegistryACL;

  private List<ACL> userAcl;

  public ResourceManagerRegistryService(String name) {
    this(name, null);
  }

  public ResourceManagerRegistryService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    userAcl = parseACLs(PERMISSIONS_REGISTRY_USERS);
  }

  /**
   * Start the service, including creating base directories with permissions
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    // create the root directories
    createRegistryPaths();
  }

  /**
   * Create the initial registry paths
   * @throws IOException any failure
   */
  @VisibleForTesting
  public void createRegistryPaths() throws IOException {
    // create the root directories
    rootRegistryACL = getACLs(KEY_REGISTRY_ZK_ACL, PERMISSIONS_REGISTRY_ROOT);
    maybeCreate("", CreateMode.PERSISTENT, rootRegistryACL, false);

    maybeCreate(PATH_USERS, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_USERS), false);
    maybeCreate(PATH_SYSTEM_SERVICES, CreateMode.PERSISTENT,
        parseACLs(PERMISSIONS_REGISTRY_SYSTEM), false);
  }

  /**
   * Create the path for a user
   * @param username username
   * @throws IOException any failure
   */
  public void createUserPath(String username) throws IOException {
    String path = BindingUtils.userPath(username);
    maybeCreate(path, CreateMode.PERSISTENT,
        createAclForUser(username), false);
  }


  /**
   * Set up the ACL for the user.
   * @param username user name
   * @return an ACL list
   * @throws IOException ACL creation/parsing problems
   */
  private List<ACL> createAclForUser(String username) throws IOException {
    return userAcl;
  }


  public void onApplicationAccepted() throws IOException {
    
  }

  public void onApplicationLaunched() throws IOException {
    
  }

  /**
   * Actions to take as an AM registers itself with the RM. 
   * @param user user owning the application
   * @param attemptId attempt ID
   * @throws IOException
   */
  public void onApplicationMasterRegistered(String user,
      ApplicationAttemptId attemptId) throws IOException {
    createUserPath(user);
  }
  
  public void onContainerCompleted() throws IOException {
    
  }

  public void onApplicationAttemptCompleted() throws IOException {
    
  }

  public void onApplicationCompleted() throws IOException {
    
  }


  /**
   * Policy to purge entries
   */
  public enum PurgePolicy {
    PurgeAll,
    FailOnChildren,
    SkipOnChildren
  }
  
  /**
   * Look under a base path and purge all records —recursively.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   * 
   * @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match: no match, no delete.
   * If set to to -1 or below, " don't check"
   * @param purgePolicy what to do if there is a matching record with children
   * @return the number of calls to the zkDelete() operation. This is purely for
   * testing.
   * @throws IOException problems
   * @throws PathIsNotEmptyDirectoryException if an attempt is made to 
   */
  @VisibleForTesting
  public int purgeRecordsWithID(String path,
      String id,
      int persistencePolicyMatch,
      PurgePolicy purgePolicy,
      BackgroundCallback callback) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(path),
        "Empty 'path' argument");
    Preconditions.checkArgument(StringUtils.isNotEmpty(id),
        "Empty 'id' argument");

    // list this path's children
    RegistryPathStatus[] entries = listDir(path);

    boolean toDelete = false;
    // look at self to see if it has a service record
    try {
      ServiceRecord serviceRecord = resolve(path);
      // there is now an entry here.
      toDelete = serviceRecord.id.equals(id) 
                 && (persistencePolicyMatch < 0 
                     || serviceRecord.persistence == persistencePolicyMatch); 
    } catch (EOFException ignored) {
      // ignore
    } catch (InvalidRecordException ignored) {
      // ignore
    }

    if (toDelete && entries.length > 0) {
      LOG.debug("Match on record @ {} with children ", path);
      // there's children
      switch (purgePolicy) {
        case SkipOnChildren:
          // don't do the deletion... continue to next record
          toDelete = false;
          break;
        case PurgeAll:
          // mark for deletion
          toDelete = true;
          entries = new RegistryPathStatus[0];
          break;
        case FailOnChildren:
          throw new PathIsNotEmptyDirectoryException(path);
      }
    }

    int deleteOps = 0;
    
    if(toDelete) {
      deleteOps++;
      zkDelete(path, true, callback);
    }

    // now go through the children
    for (RegistryPathStatus status : entries) {
      deleteOps += purgeRecordsWithID(status.path,
          id,
          persistencePolicyMatch,
          purgePolicy,
          callback);
    }

    return deleteOps;
  }

}

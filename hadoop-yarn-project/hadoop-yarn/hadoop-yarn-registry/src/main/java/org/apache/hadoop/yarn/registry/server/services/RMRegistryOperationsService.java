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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.registry.client.binding.BindingUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.services.RegistryBindingSource;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
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
 * operations, including creating and cleaning up the registry. 
 * 
 * These actions are all implemented as event handlers to operations
 * which come from the RM.
 * 
 * This service is expected to be executed by a user with the permissions
 * to manipulate the entire registry,
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RMRegistryOperationsService extends RegistryOperationsService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMRegistryOperationsService.class);
  
  private List<ACL> rootRegistryACL;

  private List<ACL> userAcl;

  private PurgePolicy purgeOnCompletionPolicy = PurgePolicy.PurgeAll;
  
  public RMRegistryOperationsService(String name) {
    this(name, null);
  }

  public RMRegistryOperationsService(String name,
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
  public void createHomeDirectory(String username) throws IOException {
    String path = homeDir(username);
    maybeCreate(path, CreateMode.PERSISTENT,
        createAclForUser(username), false);
  }

  /**
   * Get the path to a user's home dir
   * @param username username
   * @return a path for services underneath
   */
  protected String homeDir(String username) {
    return BindingUtils.userPath(username);
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

  public PurgePolicy getPurgeOnCompletionPolicy() {
    return purgeOnCompletionPolicy;
  }

  public void setPurgeOnCompletionPolicy(PurgePolicy purgeOnCompletionPolicy) {
    this.purgeOnCompletionPolicy = purgeOnCompletionPolicy;
  }

  public void onApplicationAttemptRegistered(ApplicationAttemptId attemptId,
      String host,int rpcport, String trackingurl) throws IOException {
    
  }

  public void onApplicationLaunched(ApplicationAttemptId id) throws IOException {
    
  }

  /**
   * Actions to take as an AM registers itself with the RM. 
   * @param attemptId attempt ID
   * @throws IOException problems
   */
  public void onApplicationMasterRegistered(ApplicationAttemptId attemptId) throws IOException {
  }

  /**
   * Actions to take when an application attempt is completed
   * @param attemptId  application attempt ID
   * @throws IOException problems
   */
  public void onApplicationAttemptCompleted(ApplicationAttemptId attemptId) 
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Application Attempt {} completed, purging application-level records",
          attemptId);
    }
    purgeRecordsQuietly("/",
        attemptId.toString(),
        PersistencePolicies.APPLICATION_ATTEMPT);
  }

  /**
   * Actions to take when an application attempt is completed
   * @param attemptId  application  ID
   * @throws IOException problems
   */
  public void onApplicationUnregistered(ApplicationAttemptId attemptId)
      throws IOException {
    LOG.info("Application attempt {} unregistered, purging app attempt records",
        attemptId);
    purgeRecordsQuietly("/",
        attemptId.toString(),
        PersistencePolicies.APPLICATION_ATTEMPT);
  }
/**
   * Actions to take when an application is completed
   * @param id  application  ID
   * @throws IOException problems
   */
  public void onApplicationCompleted(ApplicationId id)
      throws IOException {
    LOG.info("Application {} completed, purging application-level records",
        id);
    purgeRecordsQuietly("/",
        id.toString(),
        PersistencePolicies.APPLICATION);
  }

  public void onApplicationAttemptAdded(ApplicationAttemptId appAttemptId) {
  }

  /**
   * This is the event where the user is known, so the user directory
   * can be created
   * @param applicationId application  ID
   * @param user username
   * @throws IOException problems
   */
  public void onStateStoreEvent(ApplicationId applicationId, String user) throws
      IOException {
    createHomeDirectory(user);
  }

  /**
   * Actions to take when the AM container is completed
   * @param containerId  container ID
   * @throws IOException problems
   */
  public void onAMContainerFinished(ContainerId containerId) throws IOException {
    LOG.info("AM Container {} finished, purging application attempt records",
        containerId);

    // remove all application attempt entries
    purgeRecordsQuietly("/",
        containerId.getApplicationAttemptId().toString(),
        PersistencePolicies.APPLICATION_ATTEMPT);
    
    // also treat as a container finish to remove container
    // level records for the AM container
    onContainerFinished(containerId);
  }


  /**
   * Actions to take when the AM container is completed
   * @param id  container ID
   * @throws IOException problems
   */
  public void onContainerFinished(ContainerId id) throws IOException {
    LOG.info("Container {} finished, purging container-level records",
        id);
    purgeRecordsQuietly("/",
        id.toString(),
        PersistencePolicies.CONTAINER);
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
   * Purge all matching records under a base path -logging problems at INFO.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   *  @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match: no match, no delete.
 * If set to to -1 or below, " don't check"
   */
  private void purgeRecordsQuietly(String path,
      String id,
      int persistencePolicyMatch) {
    try {
      LOG.info("Purging records under {} with ID {} and policy {}: {}",
          path, id, persistencePolicyMatch);
      purgeRecords(path, id, persistencePolicyMatch, purgeOnCompletionPolicy,
          new DeleteCompletionCallback());
    } catch (IOException e) {
      LOG.info("Error while purging records under {} with ID {} and policy {}: {}",
          path, id, persistencePolicyMatch, e, e);
    }
  }

  /**
   * Recursive operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
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
   * @throws PathIsNotEmptyDirectoryException if an entry cannot be deleted
   * as his children and the purge policy is FailOnChildren
   */
  @VisibleForTesting
  public int purgeRecords(String path,
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Match on record @ {} with children ", path);
      }
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
      deleteOps += purgeRecords(status.path,
          id,
          persistencePolicyMatch,
          purgePolicy,
          callback);
    }

    return deleteOps;
  }


  /**
   * Callback for delete operations completing
   */
  protected static class DeleteCompletionCallback implements BackgroundCallback {
    @Override
    public void processResult(CuratorFramework client,
        CuratorEvent event) throws
        Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Delete event "+ event.toString());
      }
    }
  }
  
}

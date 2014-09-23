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

package org.apache.hadoop.yarn.registry.server.integration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.registry.client.binding.BindingUtils;
import org.apache.hadoop.yarn.registry.client.services.RegistryBindingSource;
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.services.DeleteCompletionCallback;
import org.apache.hadoop.yarn.registry.server.services.RegistryAdminService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

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
@InterfaceAudience.LimitedPrivate("YARN")
@InterfaceStability.Evolving
public class RMRegistryOperationsService extends RegistryAdminService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMRegistryOperationsService.class);

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
    RegistrySecurity registrySecurity = getRegistrySecurity();
    if (registrySecurity.isSecureRegistry()) {
      ACL sasl = registrySecurity.createSaslACLFromCurrentUser(ZooDefs.Perms.ALL);
      registrySecurity.addSystemACL(sasl);
      LOG.info("Registry System ACLs:",
          RegistrySecurity.aclsToString(
          registrySecurity.getSystemACLs()));
    }
  }

  /**
   * Start the service, including creating base directories with permissions
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    // create the root directories
    createRootRegistryPaths();
  }

  /**
   * Create the initial registry paths
   * @throws IOException any failure
   */
  @VisibleForTesting
  public void createRootRegistryPaths() throws IOException {
    // create the root directories

    systemACLs = getRegistrySecurity().getSystemACLs();
    LOG.info("System ACLs {}",
        RegistrySecurity.aclsToString(systemACLs));

    maybeCreate("", CreateMode.PERSISTENT, systemACLs, false);
    maybeCreate(PATH_USERS, CreateMode.PERSISTENT,
        systemACLs, false);
    maybeCreate(PATH_SYSTEM_SERVICES,
        CreateMode.PERSISTENT,
        systemACLs, false);
  }


  /**
   * Start an async operation to create the gome path for a user
   * @param username username
   * @return the path created
   * @throws IOException any failure
   */
  @VisibleForTesting
  public String initUserRegistryAsync(final String username)
      throws IOException {
    String path = homeDir(username);
    List<ACL> acls = aclsForUser(username);
    createDirAsync(path, acls, false);
    return path;
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
   * <b>Important: this must run client-side as it needs
   * to know the id:pass tuple for a user</b>
   * @param username user name
   * @return an ACL list
   * @throws IOException ACL creation/parsing problems
   */
  private List<ACL> aclsForUser(String username) throws IOException {
    // todo, make more specific for that user. 
    // 
    return getClientAcls();
  }

  public PurgePolicy getPurgeOnCompletionPolicy() {
    return purgeOnCompletionPolicy;
  }

  public void setPurgeOnCompletionPolicy(PurgePolicy purgeOnCompletionPolicy) {
    this.purgeOnCompletionPolicy = purgeOnCompletionPolicy;
  }

  public void onApplicationAttemptRegistered(ApplicationAttemptId attemptId,
      String host, int rpcport, String trackingurl) throws IOException {

  }

  public void onApplicationLaunched(ApplicationId id) throws IOException {

  }

  /**
   * Actions to take as an AM registers itself with the RM. 
   * @param attemptId attempt ID
   * @throws IOException problems
   */
  public void onApplicationMasterRegistered(ApplicationAttemptId attemptId) throws
      IOException {
  }

  /**
   * Actions to take when the AM container is completed
   * @param containerId  container ID
   * @throws IOException problems
   */
  public void onAMContainerFinished(ContainerId containerId) throws
      IOException {
    LOG.info("AM Container {} finished, purging application attempt records",
        containerId);

    // remove all application attempt entries
    purgeAppAttemptRecords(containerId.getApplicationAttemptId());

    // also treat as a container finish to remove container
    // level records for the AM container
    onContainerFinished(containerId);
  }

  /**
   * remove all application attempt entries
   * @param attemptId attempt ID
   */
  protected void purgeAppAttemptRecords(ApplicationAttemptId attemptId) {
    purgeRecordsAsync("/",
        attemptId.toString(),
        PersistencePolicies.APPLICATION_ATTEMPT);
  }

  /**
   * Actions to take when an application attempt is completed
   * @param attemptId  application  ID
   * @throws IOException problems
   */
  public void onApplicationAttemptUnregistered(ApplicationAttemptId attemptId)
      throws IOException {
    LOG.info("Application attempt {} unregistered, purging app attempt records",
        attemptId);
    purgeAppAttemptRecords(attemptId);
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
    purgeRecordsAsync("/",
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
    initUserRegistryAsync(user);
  }

  /**
   * Actions to take when the AM container is completed
   * @param id  container ID
   * @throws IOException problems
   */
  public void onContainerFinished(ContainerId id) throws IOException {
    LOG.info("Container {} finished, purging container-level records",
        id);
    purgeRecordsAsync("/",
        id.toString(),
        PersistencePolicies.CONTAINER);
  }

  /**
   * Queue an async operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   * @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match: 
   * no match, no delete.
   * @return a future that returns the #of records deleted
   */
  @VisibleForTesting
  public Future<Integer> purgeRecordsAsync(String path,
      String id,
      int persistencePolicyMatch) {

    return purgeRecordsAsync(path,
        id, persistencePolicyMatch,
        purgeOnCompletionPolicy,
        new DeleteCompletionCallback());
  }

  /**
   * Queue an async operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   * @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match: 
   * no match, no delete.
   * @param purgePolicy how to react to children under the entry
   * @param callback an optional callback
   * @return a future that returns the #of records deleted
   */
  @VisibleForTesting
  public Future<Integer> purgeRecordsAsync(String path,
      String id,
      int persistencePolicyMatch,
      PurgePolicy purgePolicy,
      BackgroundCallback callback) {
    LOG.info(" records under {} with ID {} and policy {}: {}",
        path, id, persistencePolicyMatch);
    return submit(
        new AsyncPurge(path,
            new ByYarnPersistence(id, persistencePolicyMatch),
            purgePolicy,
            callback));
  }

  /**
   * Select an entry by the YARN persistence policy
   */
  public static class ByYarnPersistence implements NodeSelector {
    private final String id;
    private final int targetPolicy;

    public ByYarnPersistence(String id, int targetPolicy) {
      this.id = id;
      this.targetPolicy = targetPolicy;
    }

    @Override
    public boolean shouldSelect(String path,
        RegistryPathStatus registryPathStatus,
        ServiceRecord serviceRecord) {
      return serviceRecord.id.equals(id)
             && (targetPolicy < 0 || serviceRecord.persistence == targetPolicy);
    }

    @Override
    public String toString() {
      return String.format(
          "Select by ID %s and policy %d: {}",
          id, targetPolicy);
    }
  }
  
}

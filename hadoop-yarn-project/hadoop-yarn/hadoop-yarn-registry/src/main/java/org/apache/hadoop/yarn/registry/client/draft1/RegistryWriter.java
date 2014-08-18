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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;

import java.io.IOException;
import java.util.List;

/**
 * Interface to write to a registry
 */
public interface RegistryWriter  {

  public void putServiceEntry(String user,
      String serviceClass,
      String serviceName,
      ServiceRecord entry)
      throws IOException;

  public void deleteServiceEntry(String user,
      String serviceClass,
      String serviceName)
      throws IOException;

  public void putComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName,
      ServiceRecord entry,
      boolean ephemeral)
      throws IOException;

  public void deleteComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName)
      throws IOException;

  /**
   * Set the service liveness options. 
   *
   * This sets the liveness znode to either a static or ephemeral
   * node. The policy on what do do if the node already exists
   * can be set.
   *
   * <ol>
   *   <li>
   *    It is an error to create an liveness znode if the service does not exist
   *   </li>
   *   <li>
   *    If the entry exists and <code>forceDelete==false</code>, then
   *    the set operation will fail with a <code>FileAlreadyExistsException</code>.
   *   </li>
   *   <li>
   *    If <code>forceDelete==true</code>, then
   *    the set operation will repeatedly attempt to delete then
   *    set the znode until it can be set (i.e. this ZK session
   *    owns the node).
   *   </li>
   * </ol>
   * When the function returns successfully, it means that
   * at the time the node was created, the node was owned by
   * this session. As other ZK clients may also set the liveness,
   * there is no guarantee that the znode is now owned by
   * this session.
   *
   * @param user username
   * @param serviceClass service class
   * @param serviceName name of the service
   * @param ephemeral flag to indicate the node is ephemeral
   * @param forceDelete flag to indicate the existing node should
   * be force deleted.
   * @throws FileAlreadyExistsException if the entry already exists.
   * @throws IOException on any failure
   */
  public void putServiceLiveness(String user,
      String serviceClass,
      String serviceName,
      boolean ephemeral, boolean forceDelete) throws IOException;

  public void deleteServiceLiveness(String user,
      String serviceClass,
      String serviceName) throws IOException;

  List<String> listServiceClasses(String user)
      throws IOException;

  boolean serviceClassExists(String user,
      String serviceClass)
      throws IOException;

  List<String> listServices(String user,
      String serviceClass)
      throws IOException;

  boolean serviceExists(String user,
      String serviceClass,
      String serviceName) throws IOException;

  ServiceRecord getServiceInstance(String user,
      String serviceClass,
      String serviceName)
      throws IOException;

  List<String> listComponents(String user,
      String serviceClass,
      String serviceName)
      throws IOException;

  ServiceRecord getComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException;

  boolean componentExists(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException;

  /**
   * Probe for the service liveness entry existing
   * @param user
   * @param serviceClass
   * @param serviceName
   * @return
   * @throws IOException
   */
  boolean isServiceLive(String user,
      String serviceClass,
      String serviceName) throws IOException;
}

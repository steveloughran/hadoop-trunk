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

package org.apache.hadoop.yarn.registry.client.api;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.yarn.registry.client.types.ComponentEntry;
import org.apache.hadoop.yarn.registry.client.types.ServiceEntry;

import java.io.IOException;

/**
 * Interface to write to a registry
 */
public interface RegistryWriter extends RegistryReader {

  public void putServiceEntry(String user,
      String serviceClass,
      String serviceName,
      ServiceEntry entry)
      throws IOException;

  public void deleteServiceEntry(String user,
      String serviceClass,
      String serviceName)
      throws IOException;

  public void putComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName,
      ComponentEntry entry,
      boolean ephemeral)
      throws IOException;

  public void deleteComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName)
      throws IOException;

  /**
   * Set the service liveness options. 
   * It is an error to create an en
   * @param user
   * @param serviceClass
   * @param serviceName
   * @param ephemeral
   * @throws FileAlreadyExistsException if the entry already exists.
   * @throws IOException on any failure
   */
  public void putServiceLiveness(String user,
      String serviceClass,
      String serviceName, boolean ephemeral) throws IOException;
  
  public void deleteServiceLiveness(String user,
      String serviceClass,
      String serviceName) throws IOException;
}

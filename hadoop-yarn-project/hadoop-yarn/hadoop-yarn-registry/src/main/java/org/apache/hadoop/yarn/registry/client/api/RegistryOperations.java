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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.yarn.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;

import java.io.IOException;

/**
 * Registry Operations
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RegistryOperations extends Service {

  /**
   * Create a path. 
   * 
   * It is not an error if the path exists already, be it empty or not.
   * 
   * The createParents flag also requests creating the parents. 
   * As entries in the registry can hold data while still having
   * child entries, it is not an error if any of the parent path
   * elements have service records.
   * 
   * @param path path to create
   * @param createParents also create the parents.
   * @throws PathNotFoundException parent path is not in the registry.
   * @throws NoChildrenForEphemeralsException the parent is ephemeral.
   * @throws AccessControlException access permission failure.
   * @throws InvalidPathnameException path name is invalid.
   * @throws IOException Any other IO Exception.
   * @return true if the path was created, false if it existed.
   */
  boolean mkdir(String path, boolean createParents)
      throws PathNotFoundException,
      NoChildrenForEphemeralsException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * Set a service record to an entry
   * @param path path to service record
   * @param record service record service record to create/update
   * @param createFlags creation flags
   * @throws PathNotFoundException the parent path does not exist
   * @throws NoChildrenForEphemeralsException the parent is ephemeral
   * @throws FileAlreadyExistsException path exists but create flags
   * do not include "overwrite"
   * @throws AccessControlException access permission failure.
   * @throws InvalidPathnameException path name is invalid.
   * @throws IOException Any other IO Exception.
   */
  void create(String path, ServiceRecord record, int createFlags)
      throws PathNotFoundException,
      NoChildrenForEphemeralsException,
      FileAlreadyExistsException,
      AccessControlException,
      InvalidPathnameException,
      IOException;


  /**
   * Resolve the record at a path
   * @param path path to service record
   * @return the record
   * @throws PathNotFoundException path is not in the registry.
   * @throws AccessControlException security restriction.
   * @throws InvalidPathnameException the path is invalid.
   * @throws IOException Any other IO Exception
   */
  
  ServiceRecord resolve(String path) throws PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * Get the status of a path
   * @param path
   * @return
   * @throws PathNotFoundException path is not in the registry.
   * @throws AccessControlException security restriction.
   * @throws InvalidPathnameException the path is invalid.
   * @throws IOException Any other IO Exception
   */
  RegistryPathStatus stat(String path)
      throws PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * List children of a directory
   * @param path path
   * @return a possibly empty array of child entries
   * @throws PathNotFoundException path is not in the registry.
   * @throws AccessControlException security restriction.
   * @throws InvalidPathnameException the path is invalid.
   * @throws IOException Any other IO Exception
   */
  RegistryPathStatus[] listDir(String path)
      throws PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * Delete a path.
   * 
   * If the operation returns without an error then the entry has been
   * deleted.
   * @param path path delete recursively
   * @param recursive recursive flag
   * @throws PathNotFoundException path is not in the registry.
   * @throws AccessControlException security restriction.
   * @throws InvalidPathnameException the path is invalid.
   * @throws PathIsNotEmptyDirectoryException path has child entries, but
   * recursive is false.
   * @throws IOException Any other IO Exception
   * 
   * @throws IOException
   */
  void delete(String path, boolean recursive)
      throws PathNotFoundException,
      PathIsNotEmptyDirectoryException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

}
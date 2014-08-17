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
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.yarn.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;

import java.io.FileNotFoundException;
import java.io.IOException;


public interface RegistryOperations {

  /**
   * Create a directory. Return true if the directory was created, false if it
   * was already there. Any other failure (e.g. no parent) raises an exception
   * @param path
   * @return true if the directory was created, false if it
   * was already there.
   * @throws FileNotFoundException parent path is not in the registry
   * @throws NoChildrenForEphemeralsException can't create a subdir here
   * @throws AccessControlException security restriction
   * @throws IOException Any other IO Exception
   */
  boolean mkdir(String path)
      throws FileNotFoundException,
      NoChildrenForEphemeralsException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * Set a service record to an entry
   * @param path
   * @param record
   * @param force
   * @param ephemeral
   * @throws FileNotFoundException 
   * @throws AccessControlException 
   * @throws FileAlreadyExistsException path exists but force==false
   * 
   * @throws IOException
   */
  void bind(String path, ServiceRecord record, boolean force, boolean ephemeral)
      throws FileNotFoundException,
      NoChildrenForEphemeralsException,
      FileAlreadyExistsException,
      AccessControlException,
      InvalidPathnameException,
      IOException;


  /**
   * Resolve the record at a path
   * @param path path
   * @return the record
   * @throws FileNotFoundException path is not in the registry
   * @throws AccessControlException security restriction
   * @throws IOException Any other IO Exception
   */
  
  ServiceRecord resolve(String path) throws FileNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * Get the status of a path
   * @param path
   * @return
   * @throws FileNotFoundException
   * @throws AccessControlException
   * @throws IOException
   */
  RegistryPathStatus stat(String path)
      throws FileNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * List children of a directory
   * @param path path
   * @return a possibly empty array of child entries
   * @throws FileNotFoundException
   * @throws AccessControlException
   * @throws IOException
   */
  RegistryPathStatus[] listDir(String path)
      throws FileNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

  /**
   * Delete a path 
   * If the operation returns without an error then the entry has been
   * deleted.
   * @param path path delete recursively
   * @param recursive recursive flag
   * @throws FileNotFoundException
   * @throws AccessControlException
   * @throws PathIsNotEmptyDirectoryException : path has child entries, but
   * recursive is false.
   * 
   * @throws IOException
   */
  void delete(String path, boolean recursive)
      throws FileNotFoundException,
      PathIsNotEmptyDirectoryException,
      AccessControlException,
      InvalidPathnameException,
      IOException;

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

/**
 * Options for contract tests: keys for FS-specific values, 
 * defaults.
 */
public interface ContractOptions {

  /**
   * Is a filesystem case sensitive. 
   * Some of the filesystems that say "no" here may mean
   * that it varies from platform to platform -the localfs being the key
   * example. 
   */
  String IS_CASE_SENSITIVE = "is-case-sensitive";

  /**
   * Blobstore flag. Implies it's not a real directory tree and
   * consistency is below that which Hadoop expects
   */
  String IS_BLOBSTORE = "is-blobstore";

  /**
   * @{value}
   */
  String SUPPORTS_APPEND = "supports-append";

  /**
   * @{value}
   */
  String SUPPORTS_ATOMIC_RENAME = "supports-atomic-rename";

  /**
   * @{value}
   */
  String SUPPORTS_ATOMIC_DIRECTORY_DELETE = "supports-atomic-directory-delete";

  /**
   * Does the FS support multiple block locations?
   * @{value}
   */
  String SUPPORTS_BLOCK_LOCALITY = "supports-block-locality";

  /**
   * Does the FS support multiple block locations?
   * @{value}
   */
  String SUPPORTS_CONTAT = "supports-concat";

  /**
   * Is seeking supported at all?
   * @{value}
   */
  String SUPPORTS_SEEK = "supports-seek";

  /**
   * Is seeking past the EOF supported? Some filesystems only raise an
   * exception later, when trying to read.
   * @{value}
   */
  String SUPPORTS_SEEK_PAST_EOF = "supports-seek-past-eof";

  /**
   * @{value}
   */
  String SUPPORTS_UNIX_PERMISSIONS = "supports-unix-permissions";

  /**
   * Maximum path length
   * @{value}
   */
  String MAX_PATH_ = "max-path";

  /**
   * Maximum filesize: 0 or -1 for no limit
   * @{value}
   */
  String MAX_FILESIZE = "max-filesize";

  /**
   * @{value}
   */
  String TEST_ROOT_TESTS_ENABLED = "test.root-tests-enabled";

  /**
   * Limit for #of random seeks to perform.
   * Keep low for remote filesystems for faster tests
   */
  String TEST_RANDOM_SEEK_COUNT = "test.random-seek-count";

}
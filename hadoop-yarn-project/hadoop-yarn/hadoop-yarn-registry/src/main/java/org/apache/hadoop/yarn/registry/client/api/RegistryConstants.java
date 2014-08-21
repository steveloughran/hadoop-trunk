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

public interface RegistryConstants {
  String REGISTRY_PREFIX = "yarn.registry";

  /**
   * List of hostname:port pairs defining the ZK quorum
   */
  String REGISTRY_ZK_QUORUM = REGISTRY_PREFIX + ".zk.quorum";

  /**
   * Zookeeper session timeout in milliseconds
   */
  String REGISTRY_ZK_SESSION_TIMEOUT = REGISTRY_PREFIX + ".zk.session-timeout-ms";

  /**
   * Zookeeper connect retry count
   */
  String REGISTRY_ZK_RETRY_TIMES = REGISTRY_PREFIX + ".zk.retry.times";

  /**
   * Zookeeper connection timeout in milliseconds
   */

  String REGISTRY_ZK_CONNECTION_TIMEOUT = REGISTRY_PREFIX + ".zk.connection-timeout-ms";
  
  /**
   * Zookeeper connect interval in milliseconds
   */
  String REGISTRY_ZK_RETRY_INTERVAL = REGISTRY_PREFIX + ".zk.retry.interval-ms";
  /**
   * Zookeeper retry limit in milliseconds
   */
  String REGISTRY_ZK_RETRY_CEILING = REGISTRY_PREFIX + ".zk.retry.ceiling-ms";
  String REGISTRY_ZK_ROOT = REGISTRY_PREFIX + ".zk.root";
  String REGISTRY_ZK_ACL = REGISTRY_PREFIX + ".zk.acl";

  String REGISTRY_URI_CONF = REGISTRY_PREFIX + ".uri";
  String REGISTRY_PROXY_ADDRESS_CONF = REGISTRY_PREFIX + ".proxy.address";
/*
  String REGISTRY_STORAGE_CLASS_CONF = REGISTRY_PREFIX + ".storage.class";
  String REGISTRY_ENCRYPTOR_CLASS_CONF = REGISTRY_PREFIX + ".encryptor.class";

  String REGISTRY_STORAGE_ATTRIBUTE = REGISTRY_PREFIX + ".storage";
  String REGISTRY_ENCRYPTOR_ATTRIBUTE = REGISTRY_PREFIX + ".encryptor";

  String REGISTRY_REGISTRY_HTTP_PLUGINS = REGISTRY_PREFIX + ".plugins";

  */

  String DEFAULT_ZK_HOSTS = "localhost:2181";
  int DEFAULT_ZK_SESSION_TIMEOUT = 20000;
  int DEFAULT_ZK_CONNECTION_TIMEOUT = 15000;
  int DEFAULT_ZK_RETRY_TIMES = 5;
  int DEFAULT_ZK_RETRY_INTERVAL = 1000;
  int DEFAULT_ZK_RETRY_CEILING = 20;
  String DEFAULT_REGISTRY_ROOT = "/yarnRegistry";
  
  /**
   * Pattern of a hostname 
   */
  String HOSTNAME_PATTERN =
      "([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";
  String PATH_USERS = "users/";
  String PATH_SYSTEM_SERVICES_PATH = "services/";
  String SUBPATH_COMPONENTS = "/components";
}

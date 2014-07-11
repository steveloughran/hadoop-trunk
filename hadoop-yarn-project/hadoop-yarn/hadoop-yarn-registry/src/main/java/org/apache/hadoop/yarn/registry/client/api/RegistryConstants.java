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
  String ZK_HOSTS = "yarn.registry.zk.connect-hosts";
  String ZK_SESSION_TIMEOUT =
      "yarn.registry.zk.session-timeout-ms";
  String ZK_CONNECTION_TIMEOUT =
      "yarn.registry.zk.connection-timeout-ms";
  String ZK_ROOT = "yarn.registry.zk.root-node";
  String ZK_ACL = "yarn.registry.zk.acl";
  String ZK_RETRY_TIMES = "yarn.registry.zk.retry.times";
  String ZK_RETRY_INTERVAL =
      "yarn.registry.zk.retry.interval-ms";
  String ZK_RETRY_CEILING =
      "yarn.registry.zk.retry.ceiling-ms";

  String REGISTRY_URI_CONF = "yarn.registry.uri";
  String REGISTRY_PROXY_ADDRESS_CONF =
      "yarn.registry.proxy.address";
  String REGISTRY_STORAGE_CLASS_CONF =
      "yarn.registry.storage.class";
  String REGISTRY_ENCRYPTOR_CLASS_CONF =
      "yarn.registry.encryptor.class";

  String STORAGE_ATTRIBUTE = "yarn.registry.storage";
  String ENCRYPTOR_ATTRIBUTE = "yarn.registry.encryptor";

  String REGISTRY_HTTP_PLUGINS = "yarn.registry.plugins";

  String DEFAULT_ZK_HOSTS = "localhost:2181";
  int DEFAULT_ZK_SESSION_TIMEOUT = 20000;
  int DEFAULT_ZK_CONNECTION_TIMEOUT = 15000;
  int DEFAULT_ZK_RETRY_TIMES = 5;
  int DEFAULT_ZK_RETRY_INTERVAL = 1000;
  int DEFAULT_ZK_RETRY_CEILING = 1000;
  String REGISTRY_ROOT = "/yarnRegistry";
  String REGISTRY_NAMESPACE = "yarnregistry";
  /**
   * Pattern of a hostname 
   */
  String HOSTNAME_PATTERN = 
      "([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";
  String COMPONENT_NAME_PATTERN = HOSTNAME_PATTERN;
  String SERVICE_NAME_PATTERN = HOSTNAME_PATTERN;
  String SERVICE_CLASS_PATTERN = HOSTNAME_PATTERN;
  String USERNAME_PATTERN = HOSTNAME_PATTERN;
  String USERS_PATH = "users/";
  String SYSTEM_PATH = "system/";
}

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
import org.apache.hadoop.yarn.registry.client.services.zk.RegistrySecurity;
import org.apache.zookeeper.ZooDefs;

/**
 * Constants for the registry, including configuration keys and default
 * values.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RegistryConstants {

  /**
   * prefix for registry configuration options: {@value}
   */
  String REGISTRY_PREFIX = "yarn.registry";

  /**
   * flag to indicate whether or not the registry should
   * be enabled: {@value}
   */
  String KEY_REGISTRY_ENABLED = REGISTRY_PREFIX + ".enabled";

  boolean DEFAULT_REGISTRY_ENABLED = false;
  
  /**
   * Flag to indicate whether the ZK service should be enabled {@value}
   * in the RM
   */
  String KEY_ZKSERVICE_ENABLED = REGISTRY_PREFIX + ".zkservice.enabled";
  boolean DEFAULT_ZKSERVICE_ENABLED = false;


  /**
   * List of hostname:port pairs defining the ZK quorum: {@value}
   */
  String KEY_REGISTRY_ZK_QUORUM = REGISTRY_PREFIX + ".zk.quorum";

  /**
   * Zookeeper session timeout in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_SESSION_TIMEOUT =
      REGISTRY_PREFIX + ".zk.session-timeout-ms";

  /**
   * Zookeeper connect retry count: {@value}
   */
  String KEY_REGISTRY_ZK_RETRY_TIMES = REGISTRY_PREFIX + ".zk.retry.times";

  /**
   * Zookeeper connection timeout in milliseconds: {@value}
   */

  String KEY_REGISTRY_ZK_CONNECTION_TIMEOUT =
      REGISTRY_PREFIX + ".zk.connection-timeout-ms";

  /**
   * Zookeeper connect interval in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_RETRY_INTERVAL =
      REGISTRY_PREFIX + ".zk.retry.interval-ms";

  /**
   * Zookeeper retry limit in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_RETRY_CEILING =
      REGISTRY_PREFIX + ".zk.retry.ceiling-ms";

  /**
   * Root path in the ZK tree for the registry: {@value}
   */
  String KEY_REGISTRY_ZK_ROOT = REGISTRY_PREFIX + ".zk.root";
 
  /**
   * Root path in the ZK tree for the registry: {@value}
   */
  String KEY_REGISTRY_ZK_ACL = REGISTRY_PREFIX + ".zk.acl";

  /**
   * The default ZK quorum binding: {@value}
   */
  String DEFAULT_ZK_HOSTS = "localhost:2181";

  /**
   * The default permissions for the registry root: {@value}
   */
  String DEFAULT_REGISTRY_ROOT_PERMISSIONS = "world:anyone:rwcda";

  /**
   * System accounts for the registry: {@value}. 
   */
  String KEY_REGISTRY_SYSTEM_ACCESS = REGISTRY_PREFIX + ".system.access";

  /**
   * trimmable comma separated list of system accounts: {@value}.
   * If there is an "@" at the end of an entry it 
   * instructs the registry to append the kerberos domain.
   */
  String DEFAULT_REGISTRY_SYSTEM_ACCESS = "hadoop,yarn,mapred";

  /**
   * IPv4 address permissions for world readability
   */
  String SCHEME_IP_WORLD_READABLE = "ip:0.0.0.0/32";

  /**
   * System accounts for the registry: {@value}. 
   */
  String KEY_REGISTRY_PUBLIC_ACCESS = REGISTRY_PREFIX + ".public.access";

  /**
   * default accounts for the public access to the registry: {@value}. 
   */
  String DEFAULT_REGISTRY_PUBLIC_ACCESS = SCHEME_IP_WORLD_READABLE;


  /**
   * The default ZK session timeout: {@value}
   */
  int DEFAULT_ZK_SESSION_TIMEOUT = 20000;
  /**
   * The default ZK session timeout: {@value}
   */
  int DEFAULT_ZK_CONNECTION_TIMEOUT = 15000;
  /**
   * The default # of times to retry a ZK connection: {@value}
   */
  int DEFAULT_ZK_RETRY_TIMES = 5;
  /**
   * The default interval between connection retries: {@value}
   */
  int DEFAULT_ZK_RETRY_INTERVAL = 1000;
  /**
   * Default limit on retries: {@value}
   */
  int DEFAULT_ZK_RETRY_CEILING = 5;
  String DEFAULT_REGISTRY_ROOT = "/yarnRegistry";
  
  /**
   * Pattern of a hostname : {@value}
   */
  String HOSTNAME_PATTERN =
      "([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";
  /**
   *  path to users off the root: {@value}
   */
  String PATH_USERS = "users/";
  /**
   *  path to system services off the root : {@value}
   */
  String PATH_SYSTEM_SERVICES = "services/";

  /**
   *  path under a service record to point to components of that service:
   *  {@value}
   */
  String SUBPATH_COMPONENTS = "/components";

  /**
   * Header of a service record: {@value}
   * By making this over 12 bytes long, we can auto-determine which entries
   * in a listing are too short to contain a record without getting their data
   */
  byte[] RECORD_HEADER = {'j', 's', 'o', 'n', 
                          's', 'e','r','v','i', 'c', 'e',
                          'r','e','c'};

  /**
   * ZK servertick time: {@value}
   */
  String KEY_ZKSERVICE_TICK_TIME = REGISTRY_PREFIX + ".zkservice.ticktime";
  
  /**
   * port; 0 or below means "any": {@value}
   */
  String KEY_ZKSERVICE_PORT = REGISTRY_PREFIX + ".zkservice.port";
  
  /**
   * Directory containing data: {@value}
   */
  String KEY_ZKSERVICE_DATADIR = REGISTRY_PREFIX + ".zkservice.datadir";


  /**
   * Permissions for readers: {@value}.
   */
  int PERMISSIONS_REGISTRY_READERS = ZooDefs.Perms.READ;
  /**
   * Permissions for system services: {@value}
   */

  int PERMISSIONS_REGISTRY_SYSTEM_SERVICES =
      ZooDefs.Perms.ALL;
  /**
   * Permissions for a user's root entry: {@value}.
   * All except the admin permissions (ACL access) on a node
   */
  int PERMISSIONS_REGISTRY_USER_ROOT =
      ZooDefs.Perms.READ | ZooDefs.Perms.WRITE | ZooDefs.Perms.CREATE |
      ZooDefs.Perms.DELETE;
  /**
   * Permissions for any other user entry. Full access
   */
  int PERMISSIONS_REGISTRY_USER = ZooDefs.Perms.ALL;
}

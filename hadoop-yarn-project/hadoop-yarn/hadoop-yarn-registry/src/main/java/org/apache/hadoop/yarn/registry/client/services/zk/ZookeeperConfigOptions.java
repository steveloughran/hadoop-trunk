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

package org.apache.hadoop.yarn.registry.client.services.zk;

import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.server.ZooKeeperSaslServer;

/**
 * Some ZK-internal configuration options which 
 * are usually set via system properties.
 */
public interface ZookeeperConfigOptions {


  /**
   * This is a property which must be set to enable secure clients
   */
  String ZK_ENABLE_SASL_CLIENT = ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY;


  /**
   * Set this to the <i>short</i> name of the client
   */
  String ZK_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";

  /**
   * Set this to the <i>short</i> name of the client
   */
  String ZK_SASL_SERVER_CONTEXT = ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY;

  /**
   * Should ZK downgrade on an auth failure?
   */
  String ZK_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE =
      "zookeeper.maintain_connection_despite_sasl_failure";

  /**
   * Allow failed SASL clients
   */
  String ZK_ALLOW_FAILED_SASL_CLIENTS =
      "zookeeper.allowSaslFailedClients";
}

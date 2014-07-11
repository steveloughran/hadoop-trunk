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

package org.apache.hadoop.yarn.registry.client.types;

/**
 * some common protocol types
 */
public interface ProtocolTypes {

  String PROTOCOL_UNKNOWN = "";
  String PROTOCOL_WEBUI = "webui";
  String PROTOCOL_HADOOP_IPC = "hadoop/IPC";
  String PROTOCOL_HADOOP_IPC_PROTOBUF = "hadoop/protobuf";
  String PROTOCOL_ZOOKEEPER_BINDING = "zookeeper";
  String PROTOCOL_RESTAPI = "REST";
  String PROTOCOL_WSAPI = "WS";
  String PROTOCOL_SUN_RPC = "sunrpc";
  String PROTOCOL_THRIFT = "thrift";
  String PROTOCOL_RMI = "RMI";
  String PROTOCOL_IIOP = "IIOP";

}

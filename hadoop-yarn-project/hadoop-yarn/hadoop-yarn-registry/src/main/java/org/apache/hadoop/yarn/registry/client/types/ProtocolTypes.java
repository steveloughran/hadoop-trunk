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
 * Enum of endpoint protocols -as integers. 
 * Why integers and not enums? Cross platform serialization as JSON, and
 * better versioning. It is not an error for an endpoint to have a protocol
 * that is not recognised by a client
 */
public class ProtocolTypes {
  
  public static final int PROTOCOL_UNKNOWN  = 0;
  public static final int PROTOCOL_WEBUI  = 1;
  public static final int PROTOCOL_HADOOP_IPC  = 2;
  public static final int PROTOCOL_HADOOP_IPC_PROTOBUF = 3;
  public static final int PROTOCOL_ZOOKEEPER_BINDING  = 4;
  public static final int PROTOCOL_RESTAPI  = 5;
  public static final int PROTOCOL_WSAPI  = 6;
  public static final int PROTOCOL_SUN_RPC  = 7;
  public static final int PROTOCOL_THRIFT  = 8;
  public static final int PROTOCOL_CORBA  = 9;
  public static final int PROTOCOL_RMI  = 10;
  public static final int PROTOCOL_DOTNET  = 11;
  
  
}

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

package org.apache.hadoop.yarn.registry.client.binding;

import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;

import java.net.InetSocketAddress;

/**
 * Utils to work with registry types
 */
public class RegistryTypeUtils {

  public static Endpoint urlEndpoint(String api,
      String protocolType,
      String description,
      String... urls) {
    return new Endpoint(api, AddressTypes.ADDRESS_URI,
        protocolType, description, urls);
  }

  public static Endpoint restEndpoint(String api,
      String description,
      String... urls) {
    return urlEndpoint(api, ProtocolTypes.PROTOCOL_REST,
        description, urls);
  }

  public static Endpoint webEndpoint(String api,
      String description,
      String... urls) {
    return urlEndpoint(api, ProtocolTypes.PROTOCOL_WEBUI,
        description, urls);
  }

  public static Endpoint inetAddrEndpoint(String api,
      String protocolType,
      String description,
      String... tuples) {
    return new Endpoint(api,
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        protocolType, description, tuples);
  }

  public static Endpoint ipcEndpoint(String api,
      String description,
      boolean protobuf,
      String... addresses) {
    return new Endpoint(api,
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        protobuf ? ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF
                 : ProtocolTypes.PROTOCOL_HADOOP_IPC,
        description,
        addresses);
  }

  public static String toWireFormat(InetSocketAddress address) {
    return String.format("%s/%d", address.getHostString(), address.getPort());
  }
}

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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utils to work with registry types
 */
public class RegistryTypeUtils {

  public static Endpoint urlEndpoint(String api,
      String protocolType,
      URI... urls) {
    return new Endpoint(api, protocolType, urls); 
  }

  public static Endpoint restEndpoint(String api,
      URI... urls) {
    return urlEndpoint(api, ProtocolTypes.PROTOCOL_REST, urls);
  }

  public static Endpoint webEndpoint(String api,
      URI... urls) {
    return urlEndpoint(api, ProtocolTypes.PROTOCOL_WEBUI, urls);
  }

  public static Endpoint inetAddrEndpoint(String api,
      String protocolType,
      String hostname,
      int port) {
    return new Endpoint(api,
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        protocolType,
        RegistryTypeUtils.tuple(hostname, Integer.toString(port)));
  }

  public static Endpoint ipcEndpoint(String api,
      boolean protobuf, List<String> address) {
    return new Endpoint(api,
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        protobuf ? ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF
                 : ProtocolTypes.PROTOCOL_HADOOP_IPC,
        address
    );
  }
  
  public static List<String> tuple(String...t1) {
    return Arrays.asList(t1);
  }
  public static List<String> tuple(Object...t1) {
    List<String> l = new ArrayList<String>(t1.length);
    for (Object t : t1) {
      l.add(t.toString());
    }
    return l;
  }

  /**
   * Convert a socket address pair into a string tuple, (host, port)
   * @param address an address
   * @return an element for the address list
   */
  public static List<String> marshall(InetSocketAddress address) {
    return tuple(address.getHostString(), address.getPort());
  }
}
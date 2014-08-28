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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Description of a single service/component endpoint.
 * It is designed to be marshalled as JSON
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Endpoint {
  public String api;
  public String addressType;
  public String protocolType;
  public List<List<String>> addresses;

  public Endpoint() {
  }


  /**
   * Build an endpoint with a list of addresses
   * @param api API name
   * @param addressType address type
   * @param protocolType protocol type
   * @param addrs addresses
   */
  public Endpoint(String api,
      String addressType,
      String protocolType,
      List<String>... addrs) {
    this.api = api;
    this.addressType = addressType;
    this.protocolType = protocolType;
    this.addresses = new ArrayList<List<String>>();
    if (addrs != null) {
      Collections.addAll(addresses, addrs);
    }
  }

  /**
   * Build an endpoint from a list of URIs; each URI
   * is ASCII-encoded and added to the list of addresses.
   * @param api API name
   * @param protocolType protocol type
   * @param uris
   */
  public Endpoint(String api,
      String protocolType,
      URI... uris) {
    this.api = api;
    this.addressType = AddressTypes.ADDRESS_URI;
    
    this.protocolType = protocolType;
    List<List<String>> addrs = new ArrayList<List<String>>(uris.length);
    for (URI uri : uris) {
      ArrayList<String> elt  = new ArrayList<String>(1);
      addrs.add(RegistryTypeUtils.tuple(uri.toString()));
    }
    this.addresses = addrs;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Endpoint{");
    sb.append("api='").append(api).append('\'');
    sb.append(", addressType='").append(addressType).append('\'');
    sb.append(", protocolType='").append(protocolType).append('\'');
    if (addresses != null) {
      sb.append(", address count=").append(addresses.size());
    } else {
      sb.append(", null address list=");
    }
    sb.append('}');
    return sb.toString();
  }

  public void validate() {
    Preconditions.checkNotNull(api, "null API field");
    Preconditions.checkNotNull(addressType, "null addressType field");
    Preconditions.checkNotNull(protocolType, "null protocolType field");
    Preconditions.checkNotNull(addresses, "null addresses field");
  }
}

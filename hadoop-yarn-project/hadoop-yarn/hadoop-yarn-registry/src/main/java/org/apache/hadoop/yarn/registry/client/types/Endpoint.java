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

import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Description of a single service/component endpoint.
 * It is designed to be marshalled as JSON
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Endpoint {
  public String api;
  public String addressType;
  public String protocolType;
  public String description;
  public List<List<String>> addresses;

  public Endpoint() {
  }


  /**
   * Build an endpoint with a list of addresses
   * @param api API name
   * @param addressType address type
   * @param protocolType protocol type
   * @param description description text
   * @param addrs addresses
   */
  public Endpoint(String api,
      String addressType,
      String protocolType,
      String description,
      List<String>... addrs) {
    this.api = api;
    this.addressType = addressType;
    this.protocolType = protocolType;
    this.description = description;
    if (addrs != null) {
      this.addresses = Arrays.asList(addrs);
    } else {
      this.addresses = new ArrayList<List<String>>();
    }
  }

  /**
   * Build an endpoint from a list of URIs; each URI
   * is ASCII-encoded and added to the list of addresses.
   * @param api API name
   * @param protocolType protocol type
   * @param description description text
   * @param uris
   */
  public Endpoint(String api,
      String protocolType,
      String description,
      URI... uris) {
    this.api = api;
    this.addressType = AddressTypes.ADDRESS_URI;
    
    this.protocolType = protocolType;
    this.description = description;
    List<List<String>> addrs = new ArrayList<List<String>>(uris.length);
    for (URI uri : uris) {
      ArrayList<String> elt  = new ArrayList<String>(1);
      addrs.add(RegistryTypeUtils.tuple(uri.toASCIIString()));
    }
    this.addresses = addrs;
  }
}

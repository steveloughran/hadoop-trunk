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
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)

/**
 * JSON-marshallable description of a single component
 */
public class ServiceRecord {

  /**
   * The time the service was registered -as seen by the service making
   * the registration request.
   */
  public long registrationTime;

  /**
   * ID. For containers: container ID. For application instances, application ID.
   */
  public String id;

  public String description;

  public Map<String, Endpoint> external = new HashMap<String, Endpoint>();
  public Map<String, Endpoint> internal = new HashMap<String, Endpoint>();


  public void putExternalEndpoint(String name, Endpoint endpoint) {
    Preconditions.checkArgument(name != null);
    Preconditions.checkArgument(endpoint != null);
    external.put(name, endpoint);
  }

  public void putInternalEndpoint(String name, Endpoint endpoint) {
    Preconditions.checkArgument(name != null);
    Preconditions.checkArgument(endpoint != null);
    internal.put(name, endpoint);
  }

  public Endpoint getInternalEndpoint(String name) {
    return internal.get(name);
  }

  public Endpoint getExternalEndpoint(String name) {
    return external.get(name);
  }


}

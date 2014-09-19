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
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * JSON-marshallable description of a single component.
 * It supports the deserialization of unknown attributes, but does
 * not support their creation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServiceRecord {

  /**
   * Attribute name of the yarn persistence option.
   */
  public static final String YARN_PERSISTENCE = "yarn:persistence";

  /**
   * The time the service was registered -as seen by the service making
   * the registration request.
   */
  public long registrationTime;

  /**
   * ID. For containers: container ID. For application instances, application ID.
   */
  public String id;

  /**
   * Description string
   */
  public String description;

  /**
   * map to handle unknown attributes.
   */
  @JsonIgnore
  private Map<String, Object> otherAttributes =
      new HashMap<String, Object>(4);

  /**
   *   The persistence attribute defines when a record and any child 
   *   entries may be deleted.
   *   {@link PersistencePolicies}
   */
  @JsonProperty(YARN_PERSISTENCE)
  public int persistence = PersistencePolicies.PERMANENT;
  
  /**
   * List of endpoints intended to of use to external callers
   */
  public List<Endpoint> external = new ArrayList<Endpoint>();

  /**
   * List of internal endpoints
   */
  public List<Endpoint> internal = new ArrayList<Endpoint>();

  /**
   * A string of arbitrary data. This should only be used for small amounts of
   * data related to the binding ... any large amounts of data
   * should be published by registered service endpoints.
   */
  public String data;

  /**
   * Create a service record with no ID, description or registration time.
   * Endpoint lists are set to empty lists.
   */
  public ServiceRecord() {
  }

  /**
   * Create a service record ... sets the registration time to the current
   * system time.
   * @param id service ID
   * @param description description
   * @param persistence persistence policy
   * @param data a small amount of data to be associated with the record
   */
  public ServiceRecord(String id,
      String description,
      int persistence,
      String data) {
    this.id = id;
    this.description = description;
    this.persistence = persistence;
    this.registrationTime = System.currentTimeMillis();
    this.data = data;
  }

  /**
   * Add an external endpoint
   * @param endpoint endpoint to set
   */
  public void addExternalEndpoint(Endpoint endpoint) {
    Preconditions.checkArgument(endpoint != null);
    endpoint.validate();
    external.add(endpoint);
  }

  /**
   * Add an internal endpoint
   * @param endpoint endpoint to set
   */
  public void addInternalEndpoint(Endpoint endpoint) {
    Preconditions.checkArgument(endpoint != null);
    endpoint.validate();
    internal.add(endpoint);
  }

  /**
   * Look up an internal endpoint
   * @param api API
   * @return the endpoint or null if there was no match
   */
  public Endpoint getInternalEndpoint(String api) {
    return findByAPI(internal, api);
  }

  /**
   * Look up an external endpoint
   * @param api API
   * @return the endpoint or null if there was no match
   */
  public Endpoint getExternalEndpoint(String api) {
    return findByAPI(external, api);
  }

  /**
   * Handle unknown attributes by storing them in the
   * {@link #otherAttributes} map
   * @param key attribute name
   * @param value attribute value.
   */
  @JsonAnySetter
  public void setOtherAttribute(String key, Object value) {
    otherAttributes.put(key, value);
  }

  /**
   * The map of "other" attributes set when parsing. These
   * are not included in the JSON value of this record when it
   * is generated.
   * @return a map of any unknown attributes in the deserialized JSON.
   */
  @JsonIgnore
  public Map<String, Object> getOtherAttributes() {
    return otherAttributes;
  }

  /**
   * Find an endpoint by its API
   * @param list list
   * @param api api name
   * @return the endpoint or null if there was no match
   */
  private Endpoint findByAPI(List<Endpoint> list,  String api) {
    for (Endpoint endpoint : list) {
      if (endpoint.api.equals(api)) {
        return endpoint;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("ServiceRecord{");
    sb.append("id='").append(id).append('\'');
    sb.append(", persistence='").append(persistence).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", external endpoints: {");
    for (Endpoint endpoint : external) {
      sb.append(endpoint).append("; ");
    }
    sb.append("}, internal endpoints: {");
    for (Endpoint endpoint : internal) {
      sb.append(endpoint).append("; ");
    }

    sb.append('}');
    sb.append('}');
    return sb.toString();
  }

}

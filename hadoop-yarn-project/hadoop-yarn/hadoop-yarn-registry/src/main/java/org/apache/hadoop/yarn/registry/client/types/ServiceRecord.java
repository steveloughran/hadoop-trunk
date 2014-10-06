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
import org.apache.hadoop.yarn.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.yarn.YarnRegistryAttributes;
import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON-marshallable description of a single component.
 * It supports the deserialization of unknown attributes, but does
 * not support their creation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ServiceRecord implements Cloneable {

  /**
   * Description string
   */
  public String description;

  public int yarn_persistence = PersistencePolicies.PERMANENT;

  /**
   * map to handle unknown attributes.
   */
  @JsonIgnore
  private Map<String, Object> otherAttributes = new HashMap<String, Object>(4);

  /**
   * List of endpoints intended for use to external callers
   */
  public List<Endpoint> external = new ArrayList<Endpoint>();

  /**
   * List of endpoints for use <i>within</i> an application.
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
   * @param yarn_id service ID
   * @param description description
   * @param yarn_persistence persistence policy
   * @param data a small amount of data to be associated with the record
   */
  public ServiceRecord(String yarn_id,
      String description,
      int yarn_persistence,
      String data) {
    this.setYarn_id(yarn_id);
    this.description = description;
    this.setYarn_persistence(yarn_persistence);
    this.data = data;
  }

  /**
   * Deep cloning constructor
   * @param that service record source
   */
  public ServiceRecord(ServiceRecord that) {
    this.setYarn_id(that.getYarn_id());
    this.description = that.description;
    this.setYarn_persistence(that.getYarn_persistence());
    this.data = that.data;
    // endpoints
    List<Endpoint> src = that.internal;
    if (src != null) {
      internal = new ArrayList<Endpoint>(src.size());
      for (Endpoint endpoint : src) {
        internal.add(new Endpoint(endpoint));
      }
    }
    src = that.external;
    if (src != null) {
      external = new ArrayList<Endpoint>(src.size());
      for (Endpoint endpoint : src) {
        external.add(new Endpoint(endpoint));
      }
    }
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
  public void set(String key, Object value) {
    otherAttributes.put(key, value);
  }

  /**
   * The map of "other" attributes set when parsing. These
   * are not included in the JSON value of this record when it
   * is generated.
   * @return a map of any unknown attributes in the deserialized JSON.
   */
  @JsonAnyGetter
  public Map<String, Object> any() {
    return otherAttributes;
  }

  /**
   * Get the "other" attribute with a specific key
   * @param key key to look up
   * @return the value or null
   */
  @JsonIgnore
  public Object get(String key) {
    return otherAttributes.get(key);
  }

  /**
   * Get the "other" attribute with a specific key
   * @param key key to look up
   * @param defVal default value
   * @return the value or <code>defval</code> if the value was not present
   */
  @JsonIgnore
  public Object get(String key, Object defVal) {
    Object val = otherAttributes.get(key);
    return val != null ? val: defVal;
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
    sb.append("yarn_id='").append(getYarn_id()).append('\'');
    sb.append(", yarn_persistence=").append(getYarn_persistence());
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

    if (!otherAttributes.isEmpty()) {
      sb.append(", other attributes: {");
      for (Map.Entry<String, Object> attr : otherAttributes.entrySet()) {
        sb.append("\"").append(attr.getKey()).append("\"=\"")
          .append(attr.getValue()).append("\" ");
      }
      sb.append(", other attributes: {");
      sb.append('}');
    }
    
    sb.append('}');
    return sb.toString();
  }

  /**
   * Shallow clone: all endpoints will be shared across instances
   * @return a clone of the instance
   * @throws CloneNotSupportedException
   */
  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * ID. For containers: container ID. For application instances, application ID.
   */
  public String getYarn_id() {
    return get(YarnRegistryAttributes.YARN_ID, "").toString();
  }

  public void setYarn_id(String yarn_id) {
    set(YarnRegistryAttributes.YARN_ID, yarn_id);
  }

  /**
   *   The persistence attribute defines when a record and any child 
   *   entries may be deleted.
   *   {@link PersistencePolicies}
   */
  public int getYarn_persistence() {
    return yarn_persistence;
  }

  public void setYarn_persistence(int yarn_persistence) {
    this.yarn_persistence = yarn_persistence;
  }
}

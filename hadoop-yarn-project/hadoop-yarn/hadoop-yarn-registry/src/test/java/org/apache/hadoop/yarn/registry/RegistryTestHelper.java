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

package org.apache.hadoop.yarn.registry;

import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.common.PathUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.inetAddrEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.ipcEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.restEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.tuple;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.webEndpoint;

public class RegistryTestHelper extends Assert {
  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String USER = "devteam/";
  public static final String NAME = "hdfs";
  public static final String API_WEBHDFS = "org_apache_hadoop_namenode_webhdfs";
  public static final String API_HDFS = "org_apache_hadoop_namenode_dfs";
  public static final String USERPATH =
      "/" + RegistryConstants.PATH_USERS + USER;
  public static final String PARENT_PATH = USERPATH + SC_HADOOP + "/";
  public static final String ENTRY_PATH = PARENT_PATH + NAME;
  public static final String NNIPC = "nnipc";
  public static final String IPC2 = "IPC2";
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRegistryTest.class);
  private final RecordOperations.ServiceRecordMarshal recordMarshal =
      new RecordOperations.ServiceRecordMarshal();

  public static void assertValidZKPath(String path) {
    try {
      PathUtils.validatePath(path);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Path " + path + ": " + e, e);
    }
  }

  /**
   * General code to validate bits of a component/service entry built iwth
   * {@link #addSampleEndpoints(ServiceRecord, String)}
   * @param record instance to check
   */
  protected void validateEntry(ServiceRecord record) {
    assertNotNull("null service record", record);
    List<Endpoint> endpoints = record.external;
    assertEquals(2, endpoints.size());

    Endpoint webhdfs = findEndpoint(record, API_WEBHDFS, true, 1, 1);
    assertEquals(API_WEBHDFS, webhdfs.api);
    assertEquals(AddressTypes.ADDRESS_URI, webhdfs.addressType);
    assertEquals(ProtocolTypes.PROTOCOL_REST, webhdfs.protocolType);
    List<List<String>> addressList = webhdfs.addresses;
    List<String> url = addressList.get(0);
    String addr = url.get(0);
    assertTrue(addr.contains("http"));
    assertTrue(addr.contains(":8020"));

    Endpoint nnipc = findEndpoint(record, NNIPC, false, 1,2);
    assertEquals("wrong protocol in " + nnipc, ProtocolTypes.PROTOCOL_THRIFT,
        nnipc.protocolType);

    Endpoint ipc2 = findEndpoint(record, IPC2, false, 1,2);

    Endpoint web = findEndpoint(record, "web", true, 1, 1);
    assertEquals(1, web.addresses.size());
    assertEquals(1, web.addresses.get(0).size());
    
  }

  public void assertMatches(Endpoint endpoint,
      String addressType,
      String protocolType,
      String api) {
    assertNotNull(endpoint);
    assertEquals(addressType, endpoint.addressType);
    assertEquals(protocolType, endpoint.protocolType);
    assertEquals(api, endpoint.api);
  }

  public void assertMatches(ServiceRecord written, ServiceRecord resolved) {
    assertEquals(written.id, resolved.id);
    assertEquals(written.registrationTime, resolved.registrationTime);
    assertEquals(written.description, resolved.description);
    assertEquals(written.persistence, resolved.persistence);
  }

  /**
   * Find an endpoint in a record
   * @param record   record
   * @param api API
   * @param external external?
   * @param addressElements expected # of address elements?
   * @param addressTupleSize expected size of a type
   * @return
   */
  public Endpoint findEndpoint(ServiceRecord record,
      String api, boolean external, int addressElements, int addressTupleSize) {
    Endpoint epr = external ? record.getExternalEndpoint(api)
                            : record.getInternalEndpoint(api);
    if (epr != null) {
      assertEquals("wrong # of addresses",
          addressElements, epr.addresses.size());
      assertEquals("wrong # of elements in an address tuple",
          addressTupleSize, epr.addresses.get(0).size());
      return epr;
    }
    List<Endpoint> endpoints = external ? record.external : record.internal;
    StringBuilder builder = new StringBuilder();
    for (Endpoint endpoint : endpoints) {
      builder.append("\"").append(endpoint).append("\" ");
    }
    fail("Did not find " + api + " in endpoints " + builder);
    return null;
  }

  /**
   * Log a record
   * @param name record name
   * @param record details
   * @throws IOException only if something bizarre goes wrong marshalling
   * a record.
   */
  public void logRecord(String name, ServiceRecord record) throws
      IOException {
    LOG.info(" {} = \n{}\n", name, recordMarshal.toJson(record));
  }


  /**
   * Create a service entry with the sample endpoints
   * @param persistence persistence policy
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord buildExampleServiceEntry(int persistence) throws
      IOException,
      URISyntaxException {
    ServiceRecord record = new ServiceRecord();
    record.id = "example-0001";
    record.persistence = persistence;
    record.registrationTime = System.currentTimeMillis();
    addSampleEndpoints(record, "namenode");
    return record;
  }

  /**
   * Add some endpoints
   * @param entry entry
   */
  protected void addSampleEndpoints(ServiceRecord entry, String hostname) throws
      URISyntaxException {
    entry.addExternalEndpoint(webEndpoint("web",
        new URI("http", hostname + ":80", "/")));
    entry.addExternalEndpoint(
        restEndpoint(API_WEBHDFS,
            new URI("http", hostname + ":8020", "/")));

    Endpoint endpoint = ipcEndpoint(API_HDFS,
        true, null);
    endpoint.addresses.add(tuple(hostname, "8030"));
    entry.addInternalEndpoint(endpoint);
    InetSocketAddress localhost = new InetSocketAddress("localhost", 8050);
    entry.addInternalEndpoint(
        inetAddrEndpoint(NNIPC, ProtocolTypes.PROTOCOL_THRIFT, "localhost",
            8050));
    entry.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint(
            IPC2,
            true,
            RegistryTypeUtils.marshall(localhost)));

  }
}

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

import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.EOFException;

/**
 * Test record marshalling
 */
public class TestMarshalling extends Assert {
  @Rule
  public final Timeout testTimeout = new Timeout(10000);
  @Rule
  public TestName methodName = new TestName();
  private static RecordOperations.ServiceRecordMarshal marshal;

  @BeforeClass
  public static void setupClass() {
    marshal = new RecordOperations.ServiceRecordMarshal();
  }

  @Test
  public void testRoundTrip() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description", 0);
    byte[] bytes = marshal.toBytes(record);
    ServiceRecord r2 = marshal.fromBytes("", bytes, 0);
    assertEquals(record.id, r2.id);
    assertEquals(record.persistence, r2.persistence);
    assertEquals(record.description, r2.description);
  }

  @Test
  public void testRoundTripHeaders() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description", 1);
    byte[] bytes = marshal.toByteswithHeader(record);
    ServiceRecord r2 = marshal.fromBytesWithHeader("", bytes);
    assertEquals(record.id, r2.id);
    assertEquals(record.persistence, r2.persistence);
    assertEquals(record.description, r2.description);
  }

  @Test(expected = InvalidRecordException.class)
  public void testRoundTripBadHeaders() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description", 0);
    byte[] bytes = marshal.toByteswithHeader(record);
    bytes[1] = 0x01;
    marshal.fromBytesWithHeader("src", bytes);
  }

  @Test(expected = InvalidRecordException.class)
  public void testUnmarshallHeaderTooShort() throws Throwable {
    marshal.fromBytesWithHeader("src", new byte[]{'a'});
  }

  @Test(expected = EOFException.class)
  public void testUnmarshallNoBody() throws Throwable {
    byte[] bytes = RegistryConstants.RECORD_HEADER;
    marshal.fromBytesWithHeader("src", bytes);
  }


}

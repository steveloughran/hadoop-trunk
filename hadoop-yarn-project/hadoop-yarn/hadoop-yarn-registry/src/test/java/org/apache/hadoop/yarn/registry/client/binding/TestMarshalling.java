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

import org.apache.hadoop.yarn.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecordHeader;
import org.apache.hadoop.yarn.registry.client.types.yarn.PersistencePolicies;
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
  private static RegistryUtils.ServiceRecordMarshal marshal;

  @BeforeClass
  public static void setupClass() {
    marshal = new RegistryUtils.ServiceRecordMarshal();
  }

  @Test
  public void testRoundTrip() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description",
        PersistencePolicies.PERMANENT, null);
    byte[] bytes = marshal.toBytes(record);
    ServiceRecord r2 = marshal.fromBytes("", bytes, 0);
    assertEquals(record.getYarn_id(), r2.getYarn_id());
    assertEquals(record.getYarn_persistence(), r2.getYarn_persistence());
    assertEquals(record.description, r2.description);
  }

  @Test
  public void testRoundTripHeaders() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description",
        PersistencePolicies.CONTAINER, null);
    byte[] bytes = marshal.toByteswithHeader(record);
    ServiceRecord r2 = marshal.fromBytesWithHeader("", bytes);
    assertEquals(record.getYarn_id(), r2.getYarn_id());
    assertEquals(record.getYarn_persistence(), r2.getYarn_persistence());
    assertEquals(record.description, r2.description);
  }

  @Test(expected = NoRecordException.class)
  public void testRoundTripBadHeaders() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description",
        PersistencePolicies.APPLICATION, null);
    byte[] bytes = marshal.toByteswithHeader(record);
    bytes[1] = 0x01;
    marshal.fromBytesWithHeader("src", bytes);
  }

  @Test(expected = NoRecordException.class)
  public void testUnmarshallHeaderTooShort() throws Throwable {
    marshal.fromBytesWithHeader("src", new byte[]{'a'});
  }

  @Test(expected = EOFException.class)
  public void testUnmarshallNoBody() throws Throwable {
    byte[] bytes = ServiceRecordHeader.getData();
    marshal.fromBytesWithHeader("src", bytes);
  }


  @Test
  public void testUnknonwnFieldsRoundTrip() throws Throwable {
    ServiceRecord record = new ServiceRecord("01", "description",
        PersistencePolicies.APPLICATION_ATTEMPT, null);
    record.set("key", "value");
    record.set("intval", 2);
    assertEquals("value", record.get("key"));
    assertEquals(2, record.get("intval"));
    assertNull(record.get("null"));
    assertEquals("defval", record.get("null", "defval"));
    byte[] bytes = marshal.toByteswithHeader(record);
    ServiceRecord r2 = marshal.fromBytesWithHeader("", bytes);
    assertEquals("value", r2.get("key"));
    assertEquals(2, r2.get("intval"));
  }
}

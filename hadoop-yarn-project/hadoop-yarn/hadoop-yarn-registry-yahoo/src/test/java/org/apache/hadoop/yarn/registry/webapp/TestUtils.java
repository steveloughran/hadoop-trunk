/**
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
package org.apache.hadoop.yarn.registry.webapp;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.junit.BeforeClass;

public class TestUtils {
  private static JsonNode simpleObject;

  @BeforeClass
  public static void setup() throws Exception {
     simpleObject = Utils.parseJson(
      "{\"intKey\": 10," + 
       "\"stringKey\": \"value\", " +
       "\"stringArrayKey\":[\"a\",\"b\"], " +
       "\"objKey\":{\"subInt\":11, " +
                   "\"subString\": \"subvalue\", " +
                   "\"subArray\":[\"c\",\"d\"]}}");
  }
 
  @Test
  public void testGetString() {
    assertEquals("value", Utils.getString(simpleObject, false, "stringKey"));
    assertEquals("subvalue", Utils.getString(simpleObject, false, "objKey","subString"));
    assertNull(Utils.getString(simpleObject, true, "missingKey"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMissingString() {
    Utils.getString(simpleObject, false, "missingKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBadString() {
    Utils.getString(simpleObject, false, "intKey");
  }

  @Test
  public void testGetInt() {
    assertEquals(new Integer(10), Utils.getInt(simpleObject, false, "intKey"));
    assertEquals(new Integer(11), Utils.getInt(simpleObject, false, "objKey","subInt"));
    assertNull(Utils.getInt(simpleObject, true, "missingKey"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMissingInt() {
    Utils.getInt(simpleObject, false, "missingKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBadInt() {
    Utils.getInt(simpleObject, false, "stringKey");
  }

  @Test
  public void testGetStringList() {
    assertEquals(Arrays.asList("a","b"), Utils.getStringList(simpleObject, false, "stringArrayKey"));
    assertEquals(Arrays.asList("c","d"), Utils.getStringList(simpleObject, false, "objKey","subArray"));
    assertNull(Utils.getStringList(simpleObject, true, "missingKey"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMissingStringList() {
    Utils.getStringList(simpleObject, false, "missingKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBadStringList() {
    Utils.getStringList(simpleObject, false, "intKey");
  }
}


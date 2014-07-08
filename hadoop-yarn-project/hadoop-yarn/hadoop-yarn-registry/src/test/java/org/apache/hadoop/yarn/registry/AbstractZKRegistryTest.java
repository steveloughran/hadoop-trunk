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

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;

public class AbstractZKRegistryTest extends Assert {
  
  protected static TestingServer zookeeper;

  @Rule
  public final Timeout testTimeout = new Timeout(10000);


  @Rule
  public TestName methodName = new TestName();

  @BeforeClass
  public static void createZKServer() throws Exception {
    File zkDir = new File("target/zookeeper");
    FileUtils.deleteDirectory(zkDir);
    assertTrue(zkDir.mkdirs());
    zookeeper = new TestingServer(-1, zkDir);
  }

  @AfterClass
  public static void destroyZKServer() throws IOException {
    zookeeper.close();
  }
  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Returns the connection string to use
   *
   * @return connection string
   */
  public String getConnectString() {
    return zookeeper.getConnectString();
  }

  

}

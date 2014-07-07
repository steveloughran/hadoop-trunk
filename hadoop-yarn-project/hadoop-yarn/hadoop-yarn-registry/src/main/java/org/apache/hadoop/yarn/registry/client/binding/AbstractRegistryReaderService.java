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

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.registry.client.api.RegistryReader;
import org.apache.hadoop.yarn.registry.client.types.ServiceEntry;

import java.io.IOException;
import java.util.List;

public abstract class AbstractRegistryReaderService extends AbstractService implements
    RegistryReader {

  public AbstractRegistryReaderService(String name) {
    super(name);
  }

  @Override
  public List<String> getServiceClasses(String user) throws IOException {
    return null;
  }

  @Override
  public List<String> getServices(String user, String serviceClass) throws
      IOException {
    return null;
  }

  @Override
  public ServiceEntry getServiceEntry(String user,
      String serviceClass,
      String name) throws IOException {
    return null;
  }
}

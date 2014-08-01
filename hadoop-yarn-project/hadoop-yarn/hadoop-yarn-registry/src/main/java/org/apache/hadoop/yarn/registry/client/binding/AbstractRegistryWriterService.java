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

import org.apache.hadoop.yarn.registry.client.api.RegistryWriter;
import org.apache.hadoop.yarn.registry.client.types.ComponentEntry;
import org.apache.hadoop.yarn.registry.client.types.ServiceEntry;

import java.io.IOException;

public abstract class AbstractRegistryWriterService extends
    AbstractRegistryReaderService
    implements RegistryWriter {
  protected AbstractRegistryWriterService(String name) {
    super(name);
  }


  @Override
  public void putServiceEntry(String user,
      String serviceClass,
      String name,
      ServiceEntry entry) throws IOException {
    throw notImplemented();
  }

  @Override
  public void putServiceLiveness(String user,
      String serviceClass,
      String serviceName, boolean ephemeral, boolean forceDelete) throws IOException {
    throw notImplemented();
  }

  @Override
  public void deleteServiceEntry(String user,
      String serviceClass,
      String name) throws IOException {
    throw notImplemented();
  }

  @Override
  public void putComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName,
      ComponentEntry entry,
      boolean ephemeral) throws IOException {
    throw notImplemented();
  }

  @Override
  public void deleteComponent(String user,
      String serviceClass,
      String serviceName,
      String componentName) throws IOException {
    throw notImplemented();
  }
}
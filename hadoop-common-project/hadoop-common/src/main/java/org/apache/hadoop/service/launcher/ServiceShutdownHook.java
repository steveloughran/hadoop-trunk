/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service.launcher;

import com.google.common.base.Preconditions;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;

/**
 * JVM Shutdown hook for Service which will stop the
 * Service gracefully in case of JVM shutdown.
 * This hook uses a weak reference to the service, so
 * does not cause services to be retained after they have
 * been stopped and deferenced elsewhere.
 */
public class ServiceShutdownHook implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServiceShutdownHook.class);

  /**
   * A weak reference to the service.
   */
  private final WeakReference<Service> serviceRef;

  /**
   * The service hook
   */
  private Runnable hook;

  /**
   * Create an instance
   * @param service the service
   */
  public ServiceShutdownHook(Service service) {
    serviceRef = new WeakReference<Service>(service);
  }

  public synchronized void register(int priority) {
    unregister();
    hook = this;
    ShutdownHookManager.get().addShutdownHook(hook, priority);
  }

  public synchronized void unregister() {
    if (hook != null) {
      try {
        ShutdownHookManager.get().removeShutdownHook(hook);
      } catch (IllegalStateException e) {
        LOG.info("Failed to unregister shutdown hook: {}", e, e);
      }
      hook = null;
    }
  }

  /**
   * Shutdown handler.
   * Query the service hook reference -if it is still valid the 
   * {@link Service#stop()} operation is invoked.
   */
  @Override
  public void run() {
    shutdown();
  }

  /**
   * Shutdown operation. Subclasses may extend, but it is primarily
   * made available for testing
   * @return true if the service was stopped and no exception was raised.
   */
  protected boolean shutdown() {
    Service service;
    boolean result = false;
    synchronized (this) {
      service = serviceRef.get();
      serviceRef.clear();
    }
    if (service != null)  try {
      // Stop the  Service
      service.stop();
      result = true;
    } catch (Throwable t) {
      LOG.info("Error stopping {}: {}", service.getName(), t);
    }
    return result;
  }
}

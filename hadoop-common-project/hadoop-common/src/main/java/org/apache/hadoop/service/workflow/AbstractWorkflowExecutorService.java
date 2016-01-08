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

package org.apache.hadoop.service.workflow;

import org.apache.hadoop.service.AbstractService;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A service that hosts an executor -when the service is stopped,
 * {@link ExecutorService#shutdownNow()} is invoked.
 */
public abstract class AbstractWorkflowExecutorService extends AbstractService {

  private ExecutorService executor;

  /**
   * Construct an instance with the given name -but
   * no executor
   * @param name service name
   */
  public AbstractWorkflowExecutorService(String name) {
    this(name, null);
  }

  /**
   * Construct an instance with the given name and executor
   * @param name service name
   * @param executor exectuor
   */
  protected AbstractWorkflowExecutorService(String name,
      ExecutorService executor) {
    super(name);
    this.executor = executor;
  }

  /**
   * Get the executor
   * @return the executor
   */
  public synchronized ExecutorService getExecutor() {
    return executor;
  }

  /**
   * Set the executor. This is protected as it
   * is intended to be restricted to subclasses
   * @param executor executor
   */
  protected synchronized void setExecutor(ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Execute the runnable with the executor (which 
   * must have been created already)
   * @param runnable runnable to execute
   */
  public void execute(Runnable runnable) {
    getExecutor().execute(runnable);
  }

  /**
   * Submit a callable
   * @param callable callable
   * @param <V> type of the final get
   * @return a future to wait on
   */
  public <V> Future<V> submit(Callable<V> callable) {
    return getExecutor().submit(callable);
  }
  /**
   * Stop the service: halt the executor. 
   * @throws Exception exception.
   */
  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    stopExecutor();
  }

  /**
   * Stop the executor if it is not null.
   * This uses {@link ExecutorService#shutdownNow()}
   * and so does not block until they have completed.
   */
  protected synchronized void stopExecutor() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}

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

import org.junit.Test;

/**
 * Test the {@link TestWorkflowServiceTerminatingRunnable}
 */
public class TestWorkflowServiceTerminatingRunnable extends WorkflowServiceTestBase {

  @Test(expected = IllegalArgumentException.class)
  public void testNoservice() throws Throwable {
    new ServiceTerminatingRunnable(null, new SimpleRunnable());
  }

  @Test
  public void testBasicRun() throws Throwable {

    WorkflowCompositeService svc = run(new WorkflowCompositeService());
    ServiceTerminatingRunnable runnable = new ServiceTerminatingRunnable(svc,
        new SimpleRunnable());

    // synchronous in-thread execution
    runnable.run();
    assertStopped(svc);
  }

  @Test
  public void testFailureRun() throws Throwable {

    WorkflowCompositeService svc = run(new WorkflowCompositeService());
    ServiceTerminatingRunnable runnable =
        new ServiceTerminatingRunnable(svc, new SimpleRunnable(true));

    // synchronous in-thread execution
    runnable.run();
    assertStopped(svc);
    assertNotNull(runnable.getException());
  }

}

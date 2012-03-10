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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Test;

/**
 * Test global state changes. It is critical for all tests to clean up the
 * global listener afterwards to avoid interfering with follow-on tests.
 *
 * One listener, {@link #listener} is defined which is automatically
 * unregistered on cleanup. All other listeners must be unregistered in the
 * finally clauses of the tests.
 */
public class TestGlobalStateChangeListener extends ServiceAssert {

  BreakableStateChangeListener listener = new BreakableStateChangeListener("listener");


  private void register() {
    register(listener);
  }

  private boolean unregister() {
    return unregister(listener);
  }

  private void register(ServiceStateChangeListener l) {
    AbstractService.registerGlobalListener(l);
  }

  private boolean unregister(ServiceStateChangeListener l) {
    return AbstractService.unregisterGlobalListener(l);
  }

  @After
  public void cleanup() {
    unregister();
  }

  public void assertListenerState(BreakableStateChangeListener l,
                                  Service.STATE state) {
    assertEquals("Wrong state in " + l, state, l.getLastState());
  }

  public void assertListenerEventCount(BreakableStateChangeListener l,
                                       int count) {
    assertEquals("Wrong event count in " + l, count, l.getEventCount());
  }  

  @Test
  public void testRegisterListener() {
    register();
    assertTrue("listener not registered", unregister());
  }

  @Test
  public void testRegisterListenerTwice() {
    register();
    register();
    assertTrue("listener not registered", unregister());
    //there should be no listener to unregister the second time
    assertFalse("listener double registered", unregister());
  }

  @Test
  public void testEventHistory() {
    register();
    BreakableService service = new BreakableService();
    assertListenerState(listener, Service.STATE.NOTINITED);
    assertEquals(0, listener.getEventCount());
    service.init(new Configuration());
    assertListenerState(listener, Service.STATE.INITED);
    assertSame(service, listener.getLastService());
    assertListenerEventCount(listener, 1);

    service.start();
    assertListenerState(listener, Service.STATE.STARTED);
    assertListenerEventCount(listener, 2);

    service.stop();
    assertListenerState(listener, Service.STATE.STOPPED);
    assertListenerEventCount(listener, 3);
  }

  /**
   * This test triggers a failure in the listener - the expectation is that the
   * service has already reached it's desired state, purely because the
   * notifications take place afterwards.
   *
   * @if thrown
   */
  @Test
  public void testListenerFailure() {
    listener.setFailingState(Service.STATE.INITED);
    register();
    BreakableService service = new BreakableService();
    try {
      service.init(new Configuration());
      fail("expected service initialisation to fail");
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    //still should record its invocation
    assertListenerState(listener, Service.STATE.INITED);
    assertListenerEventCount(listener, 1);
    //service should still consider itself started
    assertServiceStateInited(service);
    service.start();
    service.stop();
  }

  /**
   * Create a chain of listeners and set one in the middle to fail; verify that
   * those in front got called, and those after did not.
   */
  @Test
  public void testListenerChain() {

    LoggingStateChangeListener logListener = new LoggingStateChangeListener();
    register(logListener);
    BreakableStateChangeListener l0 = new BreakableStateChangeListener("l0");
    register(l0);
    listener.setFailingState(Service.STATE.STARTED);
    register();
    BreakableStateChangeListener l3 = new BreakableStateChangeListener("l3");
    register(l3);

    try {
      //create and init a service.
      BreakableService service = new BreakableService();
      service.init(new Configuration());
      assertServiceStateInited(service);
      assertListenerState(l0, Service.STATE.INITED);
      assertListenerState(listener, Service.STATE.INITED);
      assertListenerState(l3, Service.STATE.INITED);

      try {
        //now start and expect that to fail
        service.start();
        fail("expected service start to fail");
      } catch (BreakableService.BrokenLifecycleEvent e) {
        //expected
      }
      //expect that listener l1 and the failing listener are in start, but 
      //not the final one
      assertServiceStateStarted(service);
      assertListenerState(l0, Service.STATE.STARTED);
      assertListenerEventCount(l0, 2);
      assertListenerState(listener, Service.STATE.STARTED);
      assertListenerEventCount(listener, 2);
      //this is the listener that is not expected to have been invoked
      assertListenerState(l3, Service.STATE.INITED);
      assertListenerEventCount(l3, 1);
      
      //stop the service
      service.stop();
      //listeners are all updated
      assertListenerEventCount(l0, 3);
      assertListenerEventCount(listener, 3);
      assertListenerEventCount(l3, 2);
    } finally {
      //can all be unregistered in any order
      unregister(logListener);
      unregister(l0);
      unregister(l3);
    }


    //for completeness, check that the listeners are all unregistered, even
    //though they were registered in a different order. 
    //rather than do this by doing unregister checks, a new service is created
    BreakableService service = new BreakableService();
    //this service is initialized
    service.init(new Configuration());
    //it is asserted that the event count has not changed for the unregistered
    //listeners
    assertListenerEventCount(l0, 3);
    assertListenerEventCount(l3, 2);
    //except for the one listener that was not unregistered, which
    //has incremented by one
    assertListenerEventCount(listener, 4);
  }


}

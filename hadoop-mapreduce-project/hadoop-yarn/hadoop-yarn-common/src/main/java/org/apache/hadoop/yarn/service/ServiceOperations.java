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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * This class contains a set of methods to work with services, especially
 * to walk them through their lifecycle.
 */
public final class ServiceOperations {
  private static final Log LOG = LogFactory.getLog(AbstractService.class);

  /**
   * The list of static state change listeners.
   * This field is deliberately not downgraded to List<> to ensure that the
   * clone operation is a public shallow copy.
   */
  private static final ArrayList<ServiceStateChangeListener> globalListeners =
      new ArrayList<ServiceStateChangeListener>();

  private ServiceOperations() {
  }

  /**
   * Verify that that a service is in a given state.
   * @param state the actual state a service is in
   * @param expectedState the desired state
   * @throws IllegalStateException if the service state is different from
   * the desired state
   */
  public static void ensureCurrentState(Service.STATE state,
                                        Service.STATE expectedState) {
    if (state != expectedState) {
      throw new IllegalStateException("For this operation, the " +
                                          "current service state must be "
                                          + expectedState
                                          + " instead of " + state);
    }
  }

  /**
   * Initialize a service.
   * <p/>
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   * @param service a service that must be in the state
   *   {@link Service.STATE#NOTINITED}
   * @param configuration the configuration to initialize the service with
   * @throws RuntimeException on a state change failure
   * @throws IllegalStateException if the service is in the wrong state
   */

  public static void init(Service service, Configuration configuration) {
    Service.STATE state = service.getServiceState();
    ensureCurrentState(state, Service.STATE.NOTINITED);
    service.init(configuration);
  }

  /**
   * Start a service.
   * <p/>
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   * @param service a service that must be in the state 
   *   {@link Service.STATE#INITED}
   * @throws RuntimeException on a state change failure
   * @throws IllegalStateException if the service is in the wrong state
   */

  public static void start(Service service) {
    Service.STATE state = service.getServiceState();
    ensureCurrentState(state, Service.STATE.INITED);
    service.start();
  }

  /**
   * Initialize then start a service.
   * <p/>
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   * @param service a service that must be in the state 
   *   {@link Service.STATE#NOTINITED}
   * @param configuration the configuration to initialize the service with
   * @throws RuntimeException on a state change failure
   * @throws IllegalStateException if the service is in the wrong state
   */
  public static void deploy(Service service, Configuration configuration) {
    init(service, configuration);
    start(service);
  }

  /**
   * Stop a service.
   * <p/>Do nothing if the service is null or not
   * in a state in which it can be/needs to be stopped.
   * <p/>
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   * @param service a service or null
   */
  public static void stop(Service service) {
    if (service != null) {
      Service.STATE state = service.getServiceState();
      if (state == Service.STATE.STARTED) {
        service.stop();
      }
    }
  }

  /**
   * Stop a service; if it is null do nothing. Exceptions are caught and
   * logged at warn level. (but not Throwables). This operation is intended to
   * be used in cleanup operations
   *
   * @param service a service; may be null
   * @return any exception that was caught; null if none was.
   */
  public static Exception stopQuietly(Service service) {
    try {
      stop(service);
    } catch (Exception e) {
      LOG.warn("When stopping the service " + service.getName()
                   + " : " + e,
               e);
      return e;
    }
    return null;
  }

  /**
   * Register a global listener at the end of the list of listeners.
   * If a listener is added more than once, the previous entry is removed
   * and the new listener appended to the current list.
   * @param l the listener
   */
  public static synchronized void registerGlobalListener(ServiceStateChangeListener l) {
    if (l == null) {
      throw new IllegalArgumentException();
    }
    synchronized (globalListeners) {
      unregisterGlobalListener(l);
      globalListeners.add(l);
    }
  }

  /**
   * Unregister a global listener, returning true if it was in the current list
   * of listeners.
   * @param l the listener
   * @return true if and only if the listener was in the list of global listeners.
   */
  public static synchronized boolean unregisterGlobalListener(ServiceStateChangeListener l) {
    synchronized (globalListeners) {
      return globalListeners.remove(l);
    }
  }

  /**
   * Notify the global listener list of a state change in a service.
   * This is invoked by {@link AbstractService}; subclasses of that class
   * do not need to invoke this method.
   * @param service the service that has changed state.
   */
  @SuppressWarnings("unchecked")
  public static void notifyGlobalListeners(Service service) {
    if (service == null) {
      throw new IllegalArgumentException();
    }
    ArrayList<ServiceStateChangeListener> listenerList;
    //shallow-clone to list so the iterator does not get confused
    //by changes to the list during notification processing.
    synchronized (globalListeners) {
      listenerList = (ArrayList<ServiceStateChangeListener>)
                      (globalListeners.clone());
    }
    //notify the listeners
    for (ServiceStateChangeListener l : listenerList) {
      l.stateChanged(service);
    }
  }

  /**
   * Package-scoped method for testing -resets the listener list.
   */
  static void resetGlobalListeners() {
    synchronized (globalListeners) {
      globalListeners.clear();
    }
  }
}

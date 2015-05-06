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

import com.google.common.base.Preconditions;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service wrapper for an external program that is launched and can/will terminate.
 * This service is notified when the subprocess terminates, and stops itself 
 * and converts a non-zero exit code into a failure exception.
 * 
 * <p>
 * Key Features:
 * <ol>
 *   <li>The property {@link #executionTimeout} can be set to set a limit
 *   on the duration of a process</li>
 *   <li>Output is streamed to the output logger provided</li>.
 *   <li>The most recent lines of output are saved to a linked list</li>.
 *   <li>A synchronous callback, {@link LongLivedProcessLifecycleEvent}, is raised on the start
 *   and finish of a process.</li>
 * </ol>
 *
 * Usage:
 * <p></p>
 * The service can be built in the constructor, {@link #WorkflowForkedProcessService(String, Map, List)},
 * or have its simple constructor used to instantiate the service, then the 
 * {@link #build(Map, List)} command used to define the environment variables
 * and list of commands to execute. One of these two options MUST be exercised
 * before calling the services's {@link #start()} method.
 * <p></p>
 * The forked process is executed in the service's {@link #serviceStart()} method;
 * if still running when the service is stopped, {@link #serviceStop()} will
 * attempt to stop it.
 * <p></p>
 * 
 * The service delegates process execution to {@link LongLivedProcess},
 * receiving callbacks via the {@link LongLivedProcessLifecycleEvent}.
 * When the service receives a callback notifying that the process has completed,
 * it calls its {@link #stop()} method. If the error code was non-zero, 
 * the service is logged as having failed.
 */
public class WorkflowForkedProcessService
    extends AbstractWorkflowExecutorService
    implements LongLivedProcessLifecycleEvent, Runnable {

  /**
   * Log for the forked master process
   */
  private static final Logger LOG =
    LoggerFactory.getLogger(WorkflowForkedProcessService.class);
  public static final String ERROR_PROCESS_NOT_SET = "Process not yet configured";

  private final AtomicBoolean processTerminated = new AtomicBoolean(false);
  private boolean processStarted = false;
  private LongLivedProcess process;
  private int executionTimeout = -1;
  private int timeoutCode = 1;

  /**
   log to log to; defaults to this service log
   */
  private Logger processLog = LOG;

  /**
   * Exit code set when the spawned process exits
   */
  private final AtomicInteger exitCode = new AtomicInteger(0);

  /**
   * Create an instance of the service
   * @param name a name
   */
  public WorkflowForkedProcessService(String name) {
    super(name);
  }

  /**
   * Create an instance of the service,  set up the process
   * @param name a name
   * @param commandList list of commands is inserted on the front
   * @param env environment variables above those generated by
   * @throws IOException IO problems
   */
  public WorkflowForkedProcessService(String name,
      Map<String, String> env,
      List<String> commandList) throws IOException {
    super(name);
    build(env, commandList);
  }

  /**
   * Start the service by starting the inner process.
   * @throws ServiceStateException if no process has been set up to launch.
   * @throws Exception
   */
  @Override //AbstractService
  protected void serviceStart() throws Exception {
    if (process == null) {
      throw new ServiceStateException(ERROR_PROCESS_NOT_SET);
    }
    //now spawn the process -expect updates via callbacks
    process.start();
  }

  @Override //AbstractService
  protected void serviceStop() throws Exception {
    //tag as completed if not already; use the current exit code
    completed(exitCode.get());
    stopForkedProcess();
  }

  /**
   * Stopped the forked process
   */
  private void stopForkedProcess() {
    if (process != null) {
      process.stop();
    }
  }

  /**
   * Set the process log. This may be null for "do not log"
   * @param processLog process log
   */
  public void setProcessLog(Logger processLog) {
    this.processLog = processLog;
    process.setProcessLog(processLog);
  }

  /**
   * Set the timeout by which time a process must have finished -or -1 for forever
   * @param timeout timeout in milliseconds
   */
  public void setTimeout(int timeout, int code) {
    this.executionTimeout = timeout;
    this.timeoutCode = code;
  }

  /**
   * Build the process to execute when the service is started
   * @param commandList list of commands is inserted on the front
   * @param env environment variables above those generated by
   * @throws IOException IO problems
   */
  public void build(Map<String, String> env,
                    List<String> commandList)
      throws IOException {
    Preconditions.checkState(process == null, "process already started");
    process = new LongLivedProcess(getName(), processLog, commandList);
    process.setLifecycleCallback(this);
    //set the env variable mapping
    process.putEnvMap(env);
  }

  @Override // notification from executed process
  public synchronized void onProcessStarted(LongLivedProcess process) {
    LOG.debug("Process has started");
    processStarted = true;
    if (executionTimeout > 0) {
      setExecutor(ServiceThreadFactory.singleThreadExecutor(getName(), true));
      execute(this);
    }
  }

  @Override  // notification from executed process
  public void onProcessExited(LongLivedProcess ps,
      int uncorrected,
      int code) {
    try {
      synchronized (this) {
        completed(code);
        //note whether or not the service had already stopped
        LOG.debug("Process has exited with exit code {}", code);
        if (code != 0) {
          reportFailure(code, getName() + " failed with code " + code);
        }
      }
    } finally {
      stop();
    }
  }

  /**
   * Report a failure by building an {@link ExitUtil.ExitException}
   * with the given exit code, then calling {@link #noteFailure(Exception)}
   * to log it as this services failure exception.
   * @param code exit code
   * @param text error text
   */
  private void reportFailure(int code, String text) {
    //error
    ExitUtil.ExitException execEx = new ExitUtil.ExitException(code, text);
    LOG.debug("Noting failure", execEx);
    noteFailure(execEx);
  }

  /**
   * handle timeout response by escalating it to a failure
   */
  @Override
  public void run() {
    try {
      synchronized (processTerminated) {
        if (!processTerminated.get()) {
          processTerminated.wait(executionTimeout);
        }
      }

    } catch (InterruptedException e) {
      LOG.info("Interrupted");
      //assume signalled; exit
    }
    //check the status; if the marker isn't true, bail
    if (!processTerminated.getAndSet(true)) {
      LOG.info("process timeout: reporting error code {}", timeoutCode);

      //timeout
      if (isInState(Service.STATE.STARTED)) {
        //trigger a failure
        stopForkedProcess();
      }
      reportFailure(timeoutCode,
          getName() + ": timeout after " + executionTimeout
                   + " millis: exit code =" + timeoutCode);
    }
  }

  /**
   * Note the process as having completed.
   * The exit code is stored, the process marked as terminated
   * -and anything synchronized on <code>processTerminated</code>
   * is notified
   * @param code exit code
   */
  protected void completed(int code) {
    LOG.debug("Completed with exit code {}", code);
    synchronized (processTerminated) {
      // set the exit first, to guarantee that it will always be valid when processTerminated holds.
      // only do this if the value is currently 0
      exitCode.compareAndSet(0, code);
      processTerminated.set(true);
      processTerminated.notify();
    }
  }

  /**
   * Is the process terminated? This is false until the process is started and then completes.
   * @return true if the process has been executed to completion.
   */
  public boolean isProcessTerminated() {
    return processTerminated.get();
  }

  /**
   * Has the process started?
   * @return true if the process started.
   */
  public synchronized boolean didProcessStart() {
    return processStarted;
  }

  /**
   * Is a process running: between started and terminated
   * @return true if the process is up.
   */
  public synchronized boolean isProcessRunning() {
    return processStarted && !isProcessTerminated();
  }


  /**
   * Get the process exit code.
   * <p>
   * If the process has not yet completed, this will be zero.
   * @return an exit code in the range -127 to +128
   */
  public int getExitCode() {
    return exitCode.get();
  }

  /**
   * Get the recent output from the process, or an empty if not defined
   * @return a possibly empty list
   */
  public List<String> getRecentOutput() {
    return process != null
           ? process.getRecentOutput()
           : new LinkedList<String>();
  }

  /**
   * Get the recent output from the process, or [] if not defined
   *
   * @param finalOutput flag to indicate "wait for the final output of the process"
   * @param duration the duration, in ms,
   * to wait for recent output to become non-empty
   * @return a possibly empty list
   */
  public List<String> getRecentOutput(boolean finalOutput, int duration) {
    if (process == null) {
      return new LinkedList<>();
    }
    return process.getRecentOutput(finalOutput, duration);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(super.toString());
    if (process != null) {
      sb.append(", ").append(process.toString());
    }
    sb.append(", processStarted=").append(processStarted);
    sb.append(", processTerminated=").append(processTerminated);
    sb.append(", exitCode=").append(exitCode);
    return sb.toString();
  }
}

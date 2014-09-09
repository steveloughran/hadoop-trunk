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

package org.apache.hadoop.yarn.applications.distributedshell;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.BindingUtils;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDistributedShell {

  private static final Log LOG =
      LogFactory.getLog(TestDistributedShell.class);

  protected MiniYARNCluster yarnCluster = null;
  protected Configuration conf = new YarnConfiguration();

  protected final static String APPMASTER_JAR =
      JarFinder.getJar(ApplicationMaster.class);

  @Before
  public void setup() throws Exception {
    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, 
        FifoScheduler.class, ResourceScheduler.class);
    conf.set("yarn.log.dir", "target");
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    if (yarnCluster == null) {
      // create a minicluster with the registry enabled
      yarnCluster = new MiniYARNCluster(
        TestDistributedShell.class.getSimpleName(), 1, 1, 1, 1, true, true);
      yarnCluster.init(conf);
      yarnCluster.start();
      NodeManager  nm = yarnCluster.getNodeManager(0);
      waitForNMToRegister(nm);
      
      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
      }
      Configuration yarnClusterConfig = yarnCluster.getConfig();
      yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
      //write the document to a buffer (not directly to the file, as that
      //can cause the file being written to get read -which will then fail.
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      yarnClusterConfig.writeXml(bytesOut);
      bytesOut.close();
      //write the bytes to the file in the classpath
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      os.write(bytesOut.toByteArray());
      os.close();
    }
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf
                .get("yarn.timeline-service.leveldb-timeline-store.path")),
            true);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @After
  public void tearDown() throws IOException {
    if (yarnCluster != null) {
      try {
        yarnCluster.stop();
      } finally {
        yarnCluster = null;
      }
    }
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf
                .get("yarn.timeline-service.leveldb-timeline-store.path")),
            true);
  }
  
  @Test(timeout=90000)
  public void testDSShell() throws Exception {

    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "2",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    };

    LOG.info("Initializing DS Client");
    final Client client = new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    final AtomicBoolean result = new AtomicBoolean(false);
    Thread t = new Thread() {
      public void run() {
        try {
          result.set(client.run());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new Configuration(yarnCluster.getConfig()));
    yarnClient.start();
    String hostName = NetUtils.getHostname();

    boolean verified = false;
    String errorMessage = "";
    while(!verified) {
      List<ApplicationReport> apps = yarnClient.getApplications();
      if (apps.size() == 0 ) {
        Thread.sleep(10);
        continue;
      }
      ApplicationReport appReport = apps.get(0);
      if(appReport.getHost().equals("N/A")) {
        Thread.sleep(10);
        continue;
      }
      errorMessage =
          "Expected host name to start with '" + hostName + "', was '"
              + appReport.getHost() + "'. Expected rpc port to be '-1', was '"
              + appReport.getRpcPort() + "'.";
      if (checkHostname(appReport.getHost()) && appReport.getRpcPort() == -1) {
        verified = true;
      }
      if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED) {
        break;
      }
    }
    Assert.assertTrue(errorMessage, verified);
    t.join();
    LOG.info("Client run completed. Result=" + result);
    Assert.assertTrue(result.get());
    
    TimelineEntities entitiesAttempts = yarnCluster
        .getApplicationHistoryServer()
        .getTimelineStore()
        .getEntities(ApplicationMaster.DSEntity.DS_APP_ATTEMPT.toString(),
            null, null, null, null, null, null, null, null);
    Assert.assertNotNull(entitiesAttempts);
    Assert.assertEquals(1, entitiesAttempts.getEntities().size());
    Assert.assertEquals(2, entitiesAttempts.getEntities().get(0).getEvents()
        .size());
    Assert.assertEquals(entitiesAttempts.getEntities().get(0).getEntityType()
        .toString(), ApplicationMaster.DSEntity.DS_APP_ATTEMPT.toString());
    TimelineEntities entities = yarnCluster
        .getApplicationHistoryServer()
        .getTimelineStore()
        .getEntities(ApplicationMaster.DSEntity.DS_CONTAINER.toString(), null,
            null, null, null, null, null, null, null);
    Assert.assertNotNull(entities);
    Assert.assertEquals(2, entities.getEntities().size());
    Assert.assertEquals(entities.getEntities().get(0).getEntityType()
        .toString(), ApplicationMaster.DSEntity.DS_CONTAINER.toString());
  }

  /*
   * NetUtils.getHostname() returns a string in the form "hostname/ip".
   * Sometimes the hostname we get is the FQDN and sometimes the short name. In
   * addition, on machines with multiple network interfaces, it runs any one of
   * the ips. The function below compares the returns values for
   * NetUtils.getHostname() accounting for the conditions mentioned.
   */
  private boolean checkHostname(String appHostname) throws Exception {

    String hostname = NetUtils.getHostname();
    if (hostname.equals(appHostname)) {
      return true;
    }

    Assert.assertTrue("Unknown format for hostname " + appHostname,
      appHostname.contains("/"));
    Assert.assertTrue("Unknown format for hostname " + hostname,
      hostname.contains("/"));

    String[] appHostnameParts = appHostname.split("/");
    String[] hostnameParts = hostname.split("/");

    return (compareFQDNs(appHostnameParts[0], hostnameParts[0]) && checkIPs(
      hostnameParts[0], hostnameParts[1], appHostnameParts[1]));
  }

  private boolean compareFQDNs(String appHostname, String hostname)
      throws Exception {
    if (appHostname.equals(hostname)) {
      return true;
    }
    String appFQDN = InetAddress.getByName(appHostname).getCanonicalHostName();
    String localFQDN = InetAddress.getByName(hostname).getCanonicalHostName();
    return appFQDN.equals(localFQDN);
  }

  private boolean checkIPs(String hostname, String localIP, String appIP)
      throws Exception {

    if (localIP.equals(appIP)) {
      return true;
    }
    boolean appIPCheck = false;
    boolean localIPCheck = false;
    InetAddress[] addresses = InetAddress.getAllByName(hostname);
    for (InetAddress ia : addresses) {
      if (ia.getHostAddress().equals(appIP)) {
        appIPCheck = true;
        continue;
      }
      if (ia.getHostAddress().equals(localIP)) {
        localIPCheck = true;
      }
    }
    return (appIPCheck && localIPCheck);

  }

  @Test(timeout=90000)
  public void testDSRestartWithPreviousRunningContainers() throws Exception {
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "1",
        "--shell_command",
        "sleep 8",
        "--master_memory",
        "512",
        "--container_memory",
        "128",
        "--keep_containers_across_application_attempts"
      };

      LOG.info("Initializing DS Client");
      Client client = new Client(TestDSFailedAppMaster.class.getName(),
        new Configuration(yarnCluster.getConfig()));

      client.init(args);
      LOG.info("Running DS Client");
      boolean result = client.run();

      LOG.info("Client run completed. Result=" + result);
      // application should succeed
      Assert.assertTrue("client failed", result);
    }

  /*
   * The sleeping period in TestDSSleepingAppMaster is set as 5 seconds.
   * Set attempt_failures_validity_interval as 2.5 seconds. It will check
   * how many attempt failures for previous 2.5 seconds.
   * The application is expected to be successful.
   */
  @Test(timeout=90000)
  public void testDSAttemptFailuresValidityIntervalSucess() throws Exception {
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "1",
        "--shell_command",
        "sleep 8",
        "--master_memory",
        "512",
        "--container_memory",
        "128",
        "--attempt_failures_validity_interval",
        "2500"
      };

      LOG.info("Initializing DS Client");
      Configuration conf = yarnCluster.getConfig();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
      Client client = new Client(TestDSSleepingAppMaster.class.getName(),
        new Configuration(conf));

      client.init(args);
      LOG.info("Running DS Client");
      boolean result = client.run();

      LOG.info("Client run completed. Result=" + result);
      // application should succeed
      Assert.assertTrue(result);
    }

  /*
   * The sleeping period in TestDSSleepingAppMaster is set as 5 seconds.
   * Set attempt_failures_validity_interval as 15 seconds. It will check
   * how many attempt failure for previous 15 seconds.
   * The application is expected to be fail.
   */
  @Test(timeout=90000)
  public void testDSAttemptFailuresValidityIntervalFailed() throws Exception {
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "1",
        "--shell_command",
        "sleep 8",
        "--master_memory",
        "512",
        "--container_memory",
        "128",
        "--attempt_failures_validity_interval",
        "15000"
      };

      LOG.info("Initializing DS Client");
      Configuration conf = yarnCluster.getConfig();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
      Client client = new Client(TestDSSleepingAppMaster.class.getName(),
        new Configuration(conf));

      client.init(args);
      LOG.info("Running DS Client");
      boolean result = client.run();

      LOG.info("Client run completed. Result=" + result);
      // application should be failed
      Assert.assertFalse(result);
    }

  @Test(timeout=90000)
  public void testDSShellWithCustomLogPropertyFile() throws Exception {
    final File basedir =
        new File("target", TestDistributedShell.class.getName());
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customLogProperty = new File(tmpDir, "custom_log4j.properties");
    if (customLogProperty.exists()) {
      customLogProperty.delete();
    }
    if(!customLogProperty.createNewFile()) {
      Assert.fail("Can not create custom log4j property file.");
    }
    PrintWriter fileWriter = new PrintWriter(customLogProperty);
    // set the output to DEBUG level
    fileWriter.write("log4j.rootLogger=debug,stdout");
    fileWriter.close();
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "3",
        "--shell_command",
        "echo",
        "--shell_args",
        "HADOOP",
        "--log_properties",
        customLogProperty.getAbsolutePath(),
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    };

    //Before run the DS, the default the log level is INFO
    final Log LOG_Client =
        LogFactory.getLog(Client.class);
    Assert.assertTrue(LOG_Client.isInfoEnabled());
    Assert.assertFalse(LOG_Client.isDebugEnabled());
    final Log LOG_AM = LogFactory.getLog(ApplicationMaster.class);
    Assert.assertTrue(LOG_AM.isInfoEnabled());
    Assert.assertFalse(LOG_AM.isDebugEnabled());

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    Assert.assertTrue(verifyContainerLog(3, null, true, "DEBUG") > 10);
    //After DS is finished, the log level should be DEBUG
    Assert.assertTrue(LOG_Client.isInfoEnabled());
    Assert.assertTrue(LOG_Client.isDebugEnabled());
    Assert.assertTrue(LOG_AM.isInfoEnabled());
    Assert.assertTrue(LOG_AM.isDebugEnabled());
  }

  public void testDSShellWithCommands() throws Exception {

    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "2",
        "--shell_command",
        "\"echo output_ignored;echo output_expected\"",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    };

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<String>();
    expectedContent.add("output_expected");
    verifyContainerLog(2, expectedContent, false, "");
  }

  @Test(timeout=90000)
  public void testDSShellWithMultipleArgs() throws Exception {
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "4",
        "--shell_command",
        "echo",
        "--shell_args",
        "HADOOP YARN MAPREDUCE HDFS",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    };

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<String>();
    expectedContent.add("HADOOP YARN MAPREDUCE HDFS");
    verifyContainerLog(4, expectedContent, false, "");
  }

  @Test(timeout=90000)
  public void testDSShellWithShellScript() throws Exception {
    final File basedir =
        new File("target", TestDistributedShell.class.getName());
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customShellScript = new File(tmpDir, "custom_script.sh");
    if (customShellScript.exists()) {
      customShellScript.delete();
    }
    if (!customShellScript.createNewFile()) {
      Assert.fail("Can not create custom shell script file.");
    }
    PrintWriter fileWriter = new PrintWriter(customShellScript);
    // set the output to DEBUG level
    fileWriter.write("echo testDSShellWithShellScript");
    fileWriter.close();
    System.out.println(customShellScript.getAbsolutePath());
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "1",
        "--shell_script",
        customShellScript.getAbsolutePath(),
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    };

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<String>();
    expectedContent.add("testDSShellWithShellScript");
    verifyContainerLog(1, expectedContent, false, "");
  }

  @Test(timeout=90000)
  public void testDSShellWithInvalidArgs() throws Exception {
    Client client = new Client(new Configuration(yarnCluster.getConfig()));

    LOG.info("Initializing DS Client with no args");
    try {
      client.init(new String[]{});
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No args"));
    }

    LOG.info("Initializing DS Client with no jar file");
    try {
      String[] args = {
          "--num_containers",
          "2",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--container_memory",
          "128"
      };
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No jar"));
    }

    LOG.info("Initializing DS Client with no shell command");
    try {
      String[] args = {
          "--jar",
          APPMASTER_JAR,
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--container_memory",
          "128"
      };
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No shell command"));
    }

    LOG.info("Initializing DS Client with invalid no. of containers");
    try {
      String[] args = {
          "--jar",
          APPMASTER_JAR,
          "--num_containers",
          "-1",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--container_memory",
          "128"
      };
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Invalid no. of containers"));
    }
    
    LOG.info("Initializing DS Client with invalid no. of vcores");
    try {
      String[] args = {
          "--jar",
          APPMASTER_JAR,
          "--num_containers",
          "2",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--master_vcores",
          "-2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1"
      };
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Invalid virtual cores specified"));
    }

    LOG.info("Initializing DS Client with --shell_command and --shell_script");
    try {
      String[] args = {
          "--jar",
          APPMASTER_JAR,
          "--num_containers",
          "2",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1",
          "--shell_script",
          "test.sh"
      };
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Can not specify shell_command option " +
          "and shell_script option at the same time"));
    }

    LOG.info("Initializing DS Client without --shell_command and --shell_script");
    try {
      String[] args = {
          "--jar",
          APPMASTER_JAR,
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1"
      };
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No shell command or shell script specified " +
          "to be executed by application master"));
    }
  }

  protected static void waitForNMToRegister(NodeManager nm)
      throws Exception {
    int attempt = 60;
    ContainerManagerImpl cm =
        ((ContainerManagerImpl) nm.getNMContext().getContainerManager());
    while (cm.getBlockNewContainerRequestsStatus() && attempt-- > 0) {
      Thread.sleep(2000);
    }
  }

  @Test(timeout=90000)
  public void testContainerLaunchFailureHandling() throws Exception {
    String[] args = {
      "--jar",
      APPMASTER_JAR,
      "--num_containers",
      "2",
      "--shell_command",
      Shell.WINDOWS ? "dir" : "ls",
      "--master_memory",
      "512",
      "--container_memory",
      "128"
    };

    LOG.info("Initializing DS Client");
    Client client = new Client(ContainerLaunchFailAppMaster.class.getName(),
      new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();

    LOG.info("Client run completed. Result=" + result);
    Assert.assertFalse(result);

  }

  @Test(timeout=90000)
  public void testDebugFlag() throws Exception {
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "2",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1",
        "--debug"
    };

    LOG.info("Initializing DS Client");
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    Assert.assertTrue(client.init(args));
    LOG.info("Running DS Client");
    Assert.assertTrue(client.run());
  }

  private int verifyContainerLog(int containerNum,
      List<String> expectedContent, boolean count, String expectedWord) {
    File logFolder =
        new File(yarnCluster.getNodeManager(0).getConfig()
            .get(YarnConfiguration.NM_LOG_DIRS,
                YarnConfiguration.DEFAULT_NM_LOG_DIRS));

    File[] listOfFiles = logFolder.listFiles();
    int currentContainerLogFileIndex = -1;
    for (int i = listOfFiles.length - 1; i >= 0; i--) {
      if (listOfFiles[i].listFiles().length == containerNum + 1) {
        currentContainerLogFileIndex = i;
        break;
      }
    }
    Assert.assertTrue(currentContainerLogFileIndex != -1);
    File[] containerFiles =
        listOfFiles[currentContainerLogFileIndex].listFiles();

    int numOfWords = 0;
    for (int i = 0; i < containerFiles.length; i++) {
      for (File output : containerFiles[i].listFiles()) {
        if (output.getName().trim().contains("stdout")) {
          BufferedReader br = null;
          List<String> stdOutContent = new ArrayList<String>();
          try {

            String sCurrentLine;
            br = new BufferedReader(new FileReader(output));
            int numOfline = 0;
            while ((sCurrentLine = br.readLine()) != null) {
              if (count) {
                if (sCurrentLine.contains(expectedWord)) {
                  numOfWords++;
                }
              } else if (output.getName().trim().equals("stdout")){
                if (! Shell.WINDOWS) {
                  Assert.assertEquals("The current is" + sCurrentLine,
                      expectedContent.get(numOfline), sCurrentLine.trim());
                  numOfline++;
                } else {
                  stdOutContent.add(sCurrentLine.trim());
                }
              }
            }
            /* By executing bat script using cmd /c,
             * it will output all contents from bat script first
             * It is hard for us to do check line by line
             * Simply check whether output from bat file contains
             * all the expected messages
             */
            if (Shell.WINDOWS && !count
                && output.getName().trim().equals("stdout")) {
              Assert.assertTrue(stdOutContent.containsAll(expectedContent));
            }
          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            try {
              if (br != null)
                br.close();
            } catch (IOException ex) {
              ex.printStackTrace();
            }
          }
        }
      }
    }
    return numOfWords;
  }
  
  @Test(timeout = 90000)
  public void testRegistryOperations() throws Exception {
    
    // create a client config with an aggressive timeout policy
    Configuration clientConf = new Configuration(yarnCluster.getConfig());
    clientConf.setInt(RegistryConstants.KEY_REGISTRY_ZK_CONNECTION_TIMEOUT, 1000);
    clientConf.setInt(RegistryConstants.KEY_REGISTRY_ZK_RETRY_TIMES, 1);
    clientConf.setInt(RegistryConstants.KEY_REGISTRY_ZK_RETRY_CEILING, 1);
    clientConf.setInt(RegistryConstants.KEY_REGISTRY_ZK_RETRY_INTERVAL, 500);
    clientConf.setInt(RegistryConstants.KEY_REGISTRY_ZK_SESSION_TIMEOUT, 2000);
    
    // create a registry operations instance
    RegistryOperationsService regOps = new RegistryOperationsService();
    regOps.init(clientConf);
    regOps.start();
    LOG.info("Registry Binding: " + regOps);
    
    // do a simple registry operation to verify that it is live
    regOps.listDir("/");

    try {
      String[] args = {
          "--jar",
          APPMASTER_JAR,
          "--num_containers",
          "1",
          "--shell_command",
          "sleep 15",
          "--master_memory",
          "512",
          "--container_memory",
          "128",
      };

      LOG.info("Initializing DS Client");
      RegistryMonitoringClient client =
          new RegistryMonitoringClient(clientConf);

      client.init(args);
      LOG.info("Running DS Client");
      boolean result;
      try {
        result = client.run();
      } finally {
        client.stop();
      }

      LOG.info("Client run completed. Result=" + result);
      
      // application should have found service records
      ServiceRecord serviceRecord = client.appAttemptRecord;
      LOG.info("Service record = " + serviceRecord);
      IOException lookupException =
          client.lookupException;
      if (serviceRecord == null && lookupException != null) {
        LOG.error("Lookup of " + client.servicePath
                  + " failed with " + lookupException, lookupException);
        throw lookupException;
      }
      
      // the app should have succeeded or returned a failure message
      if (!result) {
        Assert.fail("run returned false: " + client.failureText);
      }

      // the app-level record must have been retrieved
      Assert.assertNotNull("No application record at " + client.appRecordPath,
          client.appRecord);
      
      // sleep to let some async operations in the RM continue
      Thread.sleep(10000);
      // after the app finishes its records should have been purged
      assertDeleted(regOps, client.appRecordPath);
      assertDeleted(regOps, client.servicePath);
    } finally {
      regOps.stop();
    }
  }

  protected void assertDeleted(RegistryOperationsService regOps,
      String path) throws IOException {
    try {
      ServiceRecord record = regOps.resolve(path);
      Assert.fail("Expected the record at " + path + " to have been purged,"
                  + " but found " + record);
    } catch (PathNotFoundException expected) {
      // expected
    }
  }


  /**
   * This is a subclass of the distributed shell client which
   * monitors the registry as well as the YARN app status
   */
  private class RegistryMonitoringClient extends Client {
    private String servicePath;
    private ServiceRecord permanentRecord;
    private String permanentPath;
    private IOException lookupException;
    private ServiceRecord appAttemptRecord;
    private String appAttemptPath;

    private ServiceRecord ephemeralRecord;
    private String ephemeralPath;
    
    private ServiceRecord appRecord;
    private String appRecordPath;
    
    
    private String failureText;
    private ApplicationReport report;
    private final RegistryOperationsService regOps;

    private RegistryMonitoringClient(Configuration conf) throws Exception {
      super(conf);
      // client timeout of 30s for the test runs
      setClientTimeout(30000);
      regOps = new RegistryOperationsService();
      regOps.init(getConf());
      regOps.start();
    }

    public void stop() {
      ServiceOperations.stopQuietly(regOps);
    }
  
    
    @Override
    protected boolean monitorApplication(ApplicationId appId)
        throws YarnException, IOException {

      String username = BindingUtils.currentUser();
      String serviceClass = DSConstants.SERVICE_CLASS_DISTRIBUTED_SHELL;
      String serviceName = RegistryPathUtils.encodeYarnID(appId.toString());
      servicePath =
          BindingUtils.servicePath(username, serviceClass, serviceName);
      appAttemptPath = servicePath + "-attempt";
      ephemeralPath = servicePath + "-ephemeral";
      appRecordPath = servicePath + "-app";
      permanentPath = servicePath + "-permanent";

      YarnClient yarnClient = getYarnClient();

      while (!timedOut()) {

        // Check app status every 1 second.
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          LOG.debug("Thread sleep in monitoring loop interrupted");
        }

        // Get application report for the appId we are interested in 
        report = yarnClient.getApplicationReport(appId);

        YarnApplicationState state =
            report.getYarnApplicationState();
        switch (state) {

          case NEW:
          case NEW_SAVING:
          case SUBMITTED:
          case ACCEPTED:
            continue;

            // running, extract service records if not already done
          case RUNNING:
            try {
              permanentRecord = maybeResolve(permanentRecord, permanentPath);
              // succesfull lookup, so discard any failure
              lookupException = null;
            } catch (PathNotFoundException e) {
              lookupException = e;
            }
            appRecord = maybeResolveQuietly(appRecord, appRecordPath);
            appAttemptRecord = maybeResolveQuietly(appAttemptRecord,
                appAttemptPath);
            ephemeralRecord = maybeResolveQuietly(ephemeralRecord,
                ephemeralPath);
            continue;

          case FINISHED:
            // completed
            boolean read = permanentRecord != null;
            if (!read) {
              failureText = "Permanent record was not resolved";
            }
            return read;

          case KILLED:
            failureText = "Application Killed: " + report.getDiagnostics();
            return false;
          
          case FAILED:
            failureText = "Application Failed: " + report.getDiagnostics();
            return false;

          default:
            break;
        }

      }

      if (timedOut()) {
        failureText = "Timed out: Killing application";
        forceKillApplication(appId);
      }
      return false;
    }

    /**
     * Resolve a record if it has not been resolved already
     * @param r record
     * @param path path
     * @return r if it was non null, else the resolved record
     * @throws IOException on any failure
     */
    ServiceRecord maybeResolve(ServiceRecord r, String path) throws IOException {
      if (r == null) {
        ServiceRecord record = regOps.resolve(path);
        LOG.info("Resolved at " + r +": " + record);
        return record;
      }
      return r;
    }

    /**
     * Resolve a record if it has not been resolved already —ignoring
     * any PathNotFoundException exceptions.
     * @param r record
     * @param path path
     * @return r if it was non null, a resolved record if it was found, 
     * or null if the resolution failed with a <code>PathNotFoundException</code>
     * @throws IOException on any failure
     */
    ServiceRecord maybeResolveQuietly(ServiceRecord r, String path) throws
        IOException {
      try {
        return maybeResolve(r, path);
      } catch (PathNotFoundException ignored) {
        // ignored
      }
      return r;
    }

  } // end of class RegistryMonitoringClient
  

}


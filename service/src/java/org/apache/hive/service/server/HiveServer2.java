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

package org.apache.hive.service.server;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JvmPauseMonitor;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.cli.CommonCliOptions;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveServer2TransportMode;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.exec.mr3.MR3ZooKeeperUtils;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.events.NotificationEventPoll;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.PolicyProviderContainer;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeSynchronizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.session.ClearDanglingScratchDir;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.http.HttpServer;
import org.apache.hive.http.JdbcJarDownloadServlet;
import org.apache.hive.http.LlapServlet;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.servlet.HS2LeadershipStatus;
import org.apache.hive.service.servlet.HS2Peers;
import org.apache.hive.service.servlet.QueriesRESTfulAPIServlet;
import org.apache.hive.service.servlet.QueryProfileServlet;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static CountDownLatch deleteSignal;
  private static final Logger LOG = LoggerFactory.getLogger(HiveServer2.class);
  public static final String INSTANCE_URI_CONFIG = "hive.server2.instance.uri";
  private static final int SHUTDOWN_TIME = 60;
  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private PersistentEphemeralNode znode;
  private CuratorFramework zooKeeperClient;
  private CuratorFramework zKClientForPrivSync = null;
  private boolean deregisteredWithZooKeeper = false; // Set to true only when deregistration happens
  private HttpServer webServer; // Web UI
  private PamAuthenticator pamAuthenticator;
  private Map<String, String> confsToPublish = new HashMap<String, String>();
  private String serviceUri;
  private boolean serviceDiscovery;
  private boolean activePassiveHA;
  private LeaderLatchListener leaderLatchListener;
  private ExecutorService leaderActionsExecutorService;
  private HS2ActivePassiveHARegistry hs2HARegistry;
  private AtomicBoolean isLeader = new AtomicBoolean(false);

  // used only for MR3
  private SessionState parentSession;
  private ExecutorService watcherThreadExecutor;

  // used for testing
  // TODO: remove
  private SettableFuture<Boolean> isLeaderTestFuture = SettableFuture.create();
  private SettableFuture<Boolean> notLeaderTestFuture = SettableFuture.create();

  public HiveServer2() {
    super(HiveServer2.class.getSimpleName());
    HiveConf.setLoadHiveServer2Config(true);
  }

  @VisibleForTesting
  public HiveServer2(PamAuthenticator pamAuthenticator) {
    super(HiveServer2.class.getSimpleName());
    HiveConf.setLoadHiveServer2Config(true);
    this.pamAuthenticator = pamAuthenticator;
  }

  @VisibleForTesting
  public CLIService getCliService() {
    return this.cliService;
  }

  @VisibleForTesting
  public void setPamAuthenticator(PamAuthenticator pamAuthenticator) {
    this.pamAuthenticator = pamAuthenticator;
  }

  @VisibleForTesting
  public SettableFuture<Boolean> getIsLeaderTestFuture() {
    return isLeaderTestFuture;
  }

  @VisibleForTesting
  public SettableFuture<Boolean> getNotLeaderTestFuture() {
    return notLeaderTestFuture;
  }

  private void resetIsLeaderTestFuture() {
    isLeaderTestFuture = SettableFuture.create();
  }

  private void resetNotLeaderTestFuture() {
    notLeaderTestFuture = SettableFuture.create();
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    //Initialize metrics first, as some metrics are for initialization stuff.
    try {
      if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_METRICS_ENABLED)) {
        MetricsFactory.init(hiveConf);
      }
    } catch (Throwable t) {
      LOG.warn("Could not initiate the HiveServer2 Metrics system.  Metrics may not be reported.", t);
    }

    cliService = new CLIService(this);
    addService(cliService);
    final HiveServer2 hiveServer2 = this;
    Runnable oomHook = new Runnable() {
      @Override
      public void run() {
        hiveServer2.stop();
      }
    };

    boolean isHttpTransportMode = isHttpTransportMode(hiveConf);
    boolean isAllTransportMode = isAllTransportMode(hiveConf);
    if (isHttpTransportMode || isAllTransportMode) {
      thriftCLIService = new ThriftHttpCLIService(cliService, oomHook);
      addService(thriftCLIService);
    }
    if (!isHttpTransportMode || isAllTransportMode)  {
      thriftCLIService = new ThriftBinaryCLIService(cliService, oomHook);
      addService(thriftCLIService); //thriftCliService instance is used for zookeeper purposes
    }

    super.init(hiveConf);
    // Set host name in conf
    try {
      hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, getServerHost());
    } catch (Throwable t) {
      throw new Error("Unable to initialize HiveServer2", t);
    }

    // Initialize metadata provider class
    CalcitePlanner.initializeMetadataProviderClass();

    // Create views registry
    HiveMaterializedViewsRegistry.get().init();

    StatsSources.initialize(hiveConf);

    // Setup cache if enabled.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED)) {
      try {
        QueryResultsCache.initialize(hiveConf);
      } catch (Exception err) {
        throw new RuntimeException("Error initializing the query results cache", err);
      }
    }

    try {
      NotificationEventPoll.initialize(hiveConf);
    } catch (Exception err) {
      throw new RuntimeException("Error initializing notification event poll", err);
    }

    this.serviceDiscovery = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY);
    this.activePassiveHA = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE);

    try {
      if (serviceDiscovery) {
        serviceUri = getServerInstanceURI();
        addConfsToPublish(hiveConf, confsToPublish, serviceUri);
        if (activePassiveHA) {
          hiveConf.set(INSTANCE_URI_CONFIG, serviceUri);
          leaderLatchListener = new HS2LeaderLatchListener(this, SessionState.get());
          leaderActionsExecutorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Leader Actions Handler Thread").build());
          hs2HARegistry = HS2ActivePassiveHARegistry.create(hiveConf, false);

          watcherThreadExecutor = Executors.newSingleThreadExecutor();
        }
      }
    } catch (Exception e) {
      throw new ServiceException(e);
    }

    // Setup web UI
    final int webUIPort;
    final String webHost;
    try {
      webUIPort = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT);
      webHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_BIND_HOST);
      // We disable web UI in tests unless the test is explicitly setting a
      // unique web ui port so that we don't mess up ptests.
      boolean uiDisabledInTest = hiveConf.getBoolVar(ConfVars.HIVE_IN_TEST) &&
          (webUIPort == Integer.valueOf(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
      if (uiDisabledInTest) {
        LOG.info("Web UI is disabled in test mode since webui port was not specified");
      } else {
        if (webUIPort <= 0) {
          LOG.info("Web UI is disabled since port is set to " + webUIPort);
        } else {
          LOG.info("Starting Web UI on port "+ webUIPort);
          HttpServer.Builder builder = new HttpServer.Builder("hiveserver2");
          builder.setPort(webUIPort).setConf(hiveConf);
          builder.setHost(webHost);
          builder.setMaxThreads(
            hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_MAX_THREADS));
          builder.setAdmins(hiveConf.getVar(ConfVars.USERS_IN_ADMIN_ROLE));
          // SessionManager is initialized
          builder.setContextAttribute("hive.sm",
            cliService.getSessionManager());
          hiveConf.set("startcode",
            String.valueOf(System.currentTimeMillis()));
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL)) {
            String keyStorePath = hiveConf.getVar(
              ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH);
            if (Strings.isBlank(keyStorePath)) {
              throw new IllegalArgumentException(
                ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH.varname
                  + " Not configured for SSL connection");
            }
            builder.setKeyStorePassword(ShimLoader.getHadoopShims().getPassword(
              hiveConf, ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PASSWORD.varname));
            builder.setKeyStorePath(keyStorePath);
            builder.setUseSSL(true);
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SPNEGO)) {
            String spnegoPrincipal = hiveConf.getVar(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_PRINCIPAL);
            String spnegoKeytab = hiveConf.getVar(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_KEYTAB);
            if (Strings.isBlank(spnegoPrincipal) || Strings.isBlank(spnegoKeytab)) {
              throw new IllegalArgumentException(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_PRINCIPAL.varname
                  + "/" + ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_KEYTAB.varname
                  + " Not configured for SPNEGO authentication");
            }
            builder.setSPNEGOPrincipal(spnegoPrincipal);
            builder.setSPNEGOKeytab(spnegoKeytab);
            builder.setUseSPNEGO(true);
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS)) {
            builder.setEnableCORS(true);
            String allowedOrigins = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_ORIGINS);
            String allowedMethods = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_METHODS);
            String allowedHeaders = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_HEADERS);
            if (Strings.isBlank(allowedOrigins) || Strings.isBlank(allowedMethods) || Strings.isBlank(allowedHeaders)) {
              throw new IllegalArgumentException("CORS enabled. But " +
                ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_ORIGINS.varname + "/" +
                ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_METHODS.varname + "/" +
                ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_HEADERS.varname + "/" +
                " is not configured");
            }
            builder.setAllowedOrigins(allowedOrigins);
            builder.setAllowedMethods(allowedMethods);
            builder.setAllowedHeaders(allowedHeaders);
            LOG.info("CORS enabled - allowed-origins: {} allowed-methods: {} allowed-headers: {}", allowedOrigins,
              allowedMethods, allowedHeaders);
          }
          if(hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_XFRAME_ENABLED)){
            builder.configureXFrame(true).setXFrameOption(hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_XFRAME_VALUE));
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_PAM)) {
            if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL)) {
              String hiveServer2PamServices = hiveConf.getVar(ConfVars.HIVE_SERVER2_PAM_SERVICES);
              if (hiveServer2PamServices == null || hiveServer2PamServices.isEmpty()) {
                throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_PAM_SERVICES.varname + " are not configured.");
              }
              builder.setPAMAuthenticator(pamAuthenticator == null ? new PamAuthenticator(hiveConf) : pamAuthenticator);
              builder.setUsePAM(true);
            } else if (hiveConf.getBoolVar(ConfVars.HIVE_IN_TEST)) {
              builder.setPAMAuthenticator(pamAuthenticator == null ? new PamAuthenticator(hiveConf) : pamAuthenticator);
              builder.setUsePAM(true);
            } else {
              throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL.varname + " has false value. It is recommended to set to true when PAM is used.");
            }
          }
          if (serviceDiscovery && activePassiveHA) {
            builder.setContextAttribute("hs2.isLeader", isLeader);
            builder.setContextAttribute("hs2.failover.callback", new FailoverHandlerCallback(hs2HARegistry));
            builder.setContextAttribute("hiveconf", hiveConf);
            builder.addServlet("leader", HS2LeadershipStatus.class);
            builder.addServlet("peers", HS2Peers.class);
          }
          builder.addServlet("llap", LlapServlet.class);
          builder.addServlet("jdbcjar", JdbcJarDownloadServlet.class);
          builder.setContextRootRewriteTarget("/hiveserver2.jsp");

          webServer = builder.build();
          webServer.addServlet("query_page", "/query_page.html", QueryProfileServlet.class);
          webServer.addServlet("api", "/api/*", QueriesRESTfulAPIServlet.class);
        }
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }

    if (serviceDiscovery) {
      try {
        // TODO: Why hiveserver2 of hive4 does not call setUpZooKeeperAuth()?
        setUpZooKeeperAuth(hiveConf);
        zooKeeperClient = ZooKeeperHiveHelper.startZooKeeperClient(hiveConf, zooKeeperAclProvider, true);
      } catch (Exception e) {
        LOG.error("Error in creating ZooKeeper Client", e);
        throw new ServiceException(e);
      }
    }

    // Limits.COUNTERS_MAX should be the same in both HiveServer2 and DAGAppMaster
    TezConfiguration tezConf = new TezConfiguration(hiveConf);
    Limits.setConfiguration(tezConf);

    if (true) {
      // must call before server.start() because server.start() may start LeaderWatcher which can then be
      // triggered even before server.start() returns
      try {
        MR3SessionManagerImpl.getInstance().setup(hiveConf, zooKeeperClient);
      } catch (Exception e) {
        LOG.error("Error in setting up MR3SessionManager", e);
        throw new ServiceException(e);
      }
      //   1. serviceDiscovery == true && activePassiveHA == true: multiple HS2 instances, leader exists
      //      - use service discovery and share ApplicationID
      //      - ApplicationConnectionWatcher has been created
      //      - LeaderWatcher is created when isLeader() is called
      //   2. serviceDiscovery == true && activePassiveHA == false: multiple HS2 instances, no leader exists
      //      - only for using service discovery (without sharing ApplicationID)
      //      - ApplicationConnectionWatcher is not created
      ///     - isLeader() is never called
      //   3. serviceDiscovery == false: no ZooKeeper
      //      - same as in case 2
    }

    // Add a shutdown hook for catching SIGTERM & SIGINT
    ShutdownHookManager.addShutdownHook(() -> hiveServer2.stop());
  }

  private WMFullResourcePlan createTestResourcePlan() {
    WMFullResourcePlan resourcePlan;
    WMPool pool = new WMPool("testDefault", "llap");
    pool.setAllocFraction(1f);
    pool.setQueryParallelism(1);
    resourcePlan = new WMFullResourcePlan(
        new WMResourcePlan("testDefault"), Lists.newArrayList(pool));
    resourcePlan.getPlan().setDefaultPoolPath("testDefault");
    return resourcePlan;
  }

  public static boolean isHttpTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if (transportMode != null
        && (transportMode.equalsIgnoreCase(HiveServer2TransportMode.http.toString()))) {
      return true;
    }
    return false;
  }

  public static boolean isAllTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase(HiveServer2TransportMode.all.toString()))) {
      return true;
    }
    return false;
  }

  public static boolean isKerberosAuthMode(Configuration hiveConf) {
    String authMode = hiveConf.get(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname);
    if (authMode != null && (authMode.equalsIgnoreCase("KERBEROS"))) {
      return true;
    }
    return false;
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private final ACLProvider zooKeeperAclProvider = new ACLProvider() {

    @Override
    public List<ACL> getDefaultAcl() {
      List<ACL> nodeAcls = new ArrayList<ACL>();
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read all to the world
        nodeAcls.addAll(Ids.READ_ACL_UNSAFE);
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(Perms.ALL, Ids.AUTH_IDS));
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(Ids.OPEN_ACL_UNSAFE);
      }
      return nodeAcls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  /**
   * Adds a server instance to ZooKeeper as a znode.
   *
   * @param hiveConf
   * @throws Exception
   */
  private void addServerInstanceToZooKeeper(HiveConf hiveConf, Map<String, String> confsToPublish) throws Exception {
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    String instanceURI = getServerInstanceURI();

    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      String pathPrefix =
          ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
              + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "serverUri=" + instanceURI + ";"
              + "version=" + HiveVersionInfo.getVersion() + ";" + "sequence=";
      String znodeData = "";
      if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_PUBLISH_CONFIGS)) {
        // HiveServer2 configs that this instance will publish to ZooKeeper,
        // so that the clients can read these and configure themselves properly.

        addConfsToPublish(hiveConf, confsToPublish, instanceURI);
        // Publish configs for this instance as the data on the node
        znodeData = Joiner.on(';').withKeyValueSeparator("=").join(confsToPublish);
      } else {
        znodeData = instanceURI;
      }
      byte[] znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"));
      znode =
          new PersistentEphemeralNode(zooKeeperClient,
              PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, pathPrefix, znodeDataUTF8);
      znode.start();
      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }
      setDeregisteredWithZooKeeper(false);
      final String znodePath = znode.getActualPath();
      // Set a watch on the znode
      if (zooKeeperClient.checkExists().usingWatcher(new DeRegisterWatcher()).forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.");
      }
      LOG.info("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI);
    } catch (Exception e) {
      LOG.error("Unable to create a znode for this server instance", e);
      if (znode != null) {
        znode.close();
      }
      throw (e);
    }
  }

  /**
   * Add conf keys, values that HiveServer2 will publish to ZooKeeper.
   * @param hiveConf
   */
  private void addConfsToPublish(HiveConf hiveConf, Map<String, String> confsToPublish, String serviceUri) {
    // Hostname
    confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST));
    // Hostname:port
    confsToPublish.put(INSTANCE_URI_CONFIG, serviceUri);
    // Transport mode
    confsToPublish.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE));
    // Transport specific confs
    boolean isHttpTransportMode = isHttpTransportMode(hiveConf);
    boolean isAllTransportMode = isAllTransportMode(hiveConf);

    if (isHttpTransportMode || isAllTransportMode) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname,
          Integer.toString(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT)));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));
    }
    if (!isHttpTransportMode || isAllTransportMode) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname,
          Integer.toString(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT)));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    }
    // Auth specific confs
    confsToPublish.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION));
    if (isKerberosAuthMode(hiveConf)) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL));
    }
    // SSL conf
    confsToPublish.put(ConfVars.HIVE_SERVER2_USE_SSL.varname,
        Boolean.toString(hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)));
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param hiveConf
   * @return
   * @throws Exception
   */
  private void setUpZooKeeperAuth(HiveConf hiveConf) throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
      if (principal.isEmpty()) {
        throw new IOException("HiveServer2 Kerberos principal is empty");
      }
      String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
      if (keyTabFile.isEmpty()) {
        throw new IOException("HiveServer2 Kerberos keytab is empty");
      }
      // Install the JAAS Configuration for the runtime
      Utils.setZookeeperClientKerberosJaasConfig(principal, keyTabFile);
    }
  }

  public boolean isLeader() {
    return isLeader.get();
  }

  public int getOpenSessionsCount() {
    return cliService != null ? cliService.getSessionManager().getOpenSessionCount() : 0;
  }

  interface FailoverHandler {
    void failover() throws Exception;
  }

  public static class FailoverHandlerCallback implements FailoverHandler {
    private HS2ActivePassiveHARegistry hs2HARegistry;

    FailoverHandlerCallback(HS2ActivePassiveHARegistry hs2HARegistry) {
      this.hs2HARegistry = hs2HARegistry;
    }

    @Override
    public void failover() throws Exception {
      hs2HARegistry.failover();
    }
  }
  /**
   * The watcher class which sets the de-register flag when the znode corresponding to this server
   * instance is deleted. Additionally, it shuts down the server if there are no more active client
   * sessions at the time of receiving a 'NodeDeleted' notification from ZooKeeper.
   */
  private class DeRegisterWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
        if (znode != null) {
          try {
            znode.close();
            LOG.warn("This HiveServer2 instance is now de-registered from ZooKeeper. "
                + "The server will be shut down after the last client session completes.");
          } catch (IOException e) {
            LOG.error("Failed to close the persistent ephemeral znode", e);
          } finally {
            HiveServer2.this.setDeregisteredWithZooKeeper(true);
            // If there are no more active client sessions, stop the server
            if (cliService.getSessionManager().getOpenSessionCount() == 0) {
              LOG.warn("This instance of HiveServer2 has been removed from the list of server "
                  + "instances available for dynamic service discovery. "
                  + "The last client session has ended - will shutdown now.");
              HiveServer2.this.stop();
            }
          }
        }
      }
    }
  }

  private void removeServerInstanceFromZooKeeper() throws Exception {
    setDeregisteredWithZooKeeper(true);

    if (znode != null) {
      znode.close();
    }
    LOG.info("Server instance removed from ZooKeeper.");
  }

  public boolean isDeregisteredWithZooKeeper() {
    return deregisteredWithZooKeeper;
  }

  private void setDeregisteredWithZooKeeper(boolean deregisteredWithZooKeeper) {
    this.deregisteredWithZooKeeper = deregisteredWithZooKeeper;
  }

  private String getServerInstanceURI() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getHostName() + ":"
        + thriftCLIService.getPortNumber();
  }

  public String getServerHost() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getHostName();
  }

  @Override
  public synchronized void start() {
    super.start();
    // If we're supporting dynamic service discovery, we'll add the service uri for this
    // HiveServer2 instance to Zookeeper as a znode.
    HiveConf hiveConf = getHiveConf();
    if (serviceDiscovery) {
      try {
        assert zooKeeperClient != null;
        if (activePassiveHA) {
          hs2HARegistry.registerLeaderLatchListener(leaderLatchListener, leaderActionsExecutorService);
          hs2HARegistry.start();
          LOG.info("HS2 HA registry started");
          parentSession = SessionState.get();
          invokeApplicationConnectionWatcher();
        }
        // In the original Hive 3, if activePassiveHA == true, only the Leader HS2 receives all the traffic.
        // In contrast, in the case of Hive-MR3, 'activePassiveHA == true' means that all HS2 instances
        // share the traffic from Beeline connections in order to take advantage of a common MR3 DAGAppMaster.
        //
        // We always call addServerInstanceToZooKeeper() so that ZooKeeper can find all HS2 instances.
        addServerInstanceToZooKeeper(hiveConf, confsToPublish);
      } catch (Exception e) {
        LOG.error("Error adding this HiveServer2 instance to ZooKeeper: ", e);
        throw new ServiceException(e);
      }
    }

    try {
      startPrivilegeSynchronizer(hiveConf);
    } catch (Exception e) {
      LOG.error("Error starting priviledge synchronizer: ", e);
      throw new ServiceException(e);
    }

    if (webServer != null) {
      try {
        webServer.start();
        LOG.info("Web UI has started on port " + webServer.getPort());
      } catch (Exception e) {
        LOG.error("Error starting Web UI: ", e);
        throw new ServiceException(e);
      }
    }

    if (!activePassiveHA) {
      LOG.info("HS2 interactive HA not enabled. Starting sessions..");
    } else {
      LOG.info("HS2 interactive HA enabled. Sessions will be started/reconnected by the leader.");
    }
  }

  private static class HS2LeaderLatchListener implements LeaderLatchListener {
    private HiveServer2 hiveServer2;
    private SessionState parentSession;

    HS2LeaderLatchListener(final HiveServer2 hs2, final SessionState parentSession) {
      this.hiveServer2 = hs2;
      this.parentSession = parentSession;
    }

    // leadership status change happens inside synchronized methods LeaderLatch.setLeadership().
    // Also we use single threaded executor service for handling notifications which guarantees ordering for
    // notification handling. if a leadership status change happens when tez sessions are getting created,
    // the notLeader notification will get queued in executor service.
    @Override
    public void isLeader() {
      LOG.info("HS2 instance {} became the LEADER. Starting/Reconnecting tez sessions..", hiveServer2.serviceUri);
      hiveServer2.isLeader.set(true);
      if (parentSession != null) {
        SessionState.setCurrentSessionState(parentSession);
      }
      hiveServer2.invokeLeaderWatcher();

      // resolve futures used for testing
      if (HiveConf.getBoolVar(hiveServer2.getHiveConf(), ConfVars.HIVE_IN_TEST)) {
        hiveServer2.isLeaderTestFuture.set(true);
        hiveServer2.resetNotLeaderTestFuture();
      }
    }

    @Override
    public void notLeader() {
      LOG.info("HS2 instance {} LOST LEADERSHIP. Stopping/Disconnecting tez sessions..", hiveServer2.serviceUri);
      // do not call hiveServer2.closeHiveSessions() because there is no need to close active Beeline connections
      hiveServer2.isLeader.set(false);

      // resolve futures used for testing
      if (HiveConf.getBoolVar(hiveServer2.getHiveConf(), ConfVars.HIVE_IN_TEST)) {
        hiveServer2.notLeaderTestFuture.set(true);
        hiveServer2.resetIsLeaderTestFuture();
      }
    }
  }

  private void closeHiveSessions() {
    LOG.info("Closing all open hive sessions.");
    if (cliService != null && cliService.getSessionManager().getOpenSessionCount() > 0) {
      try {
        for (HiveSession session : cliService.getSessionManager().getSessions()) {
          cliService.getSessionManager().closeSession(session.getSessionHandle());
        }
        LOG.info("Closed all open hive sessions");
      } catch (HiveSQLException e) {
        LOG.error("Unable to close all open sessions.", e);
      }
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down HiveServer2");
    HiveConf hiveConf = this.getHiveConf();
    super.stop();

    if (serviceDiscovery && activePassiveHA
            && hiveConf != null) {
      watcherThreadExecutor.shutdown();
    }

    if (hs2HARegistry != null) {
      hs2HARegistry.stop();
      shutdownExecutor(leaderActionsExecutorService);
      LOG.info("HS2 HA registry stopped");
      hs2HARegistry = null;
    }
    if (webServer != null) {
      try {
        webServer.stop();
        LOG.info("Web UI has stopped");
      } catch (Exception e) {
        LOG.error("Error stopping Web UI: ", e);
      }
    }
    // Shutdown Metrics
    if (MetricsFactory.getInstance() != null) {
      try {
        MetricsFactory.close();
      } catch (Exception e) {
        LOG.error("error in Metrics deinit: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      }
    }
    // Remove this server instance from ZooKeeper if dynamic service discovery is set
    if (serviceDiscovery) {
      try {
        removeServerInstanceFromZooKeeper();
      } catch (Exception e) {
        LOG.error("Error removing znode for this HiveServer2 instance from ZooKeeper.", e);
      }
    }

    if (hiveConf != null) {
      try {
        MR3SessionManagerImpl.getInstance().shutdown();
      } catch(Exception ex) {
        LOG.error("MR3 session pool manager failed to stop during HiveServer2 shutdown.", ex);
      }
    }

    if (zooKeeperClient != null) {
      zooKeeperClient.close();
    }

    if (zKClientForPrivSync != null) {
      zKClientForPrivSync.close();
    }
  }

  private void shutdownExecutor(final ExecutorService leaderActionsExecutorService) {
    leaderActionsExecutorService.shutdown();
    try {
      if (!leaderActionsExecutorService.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
        LOG.warn("Executor service did not terminate in the specified time {} sec", SHUTDOWN_TIME);
        List<Runnable> droppedTasks = leaderActionsExecutorService.shutdownNow();
        LOG.warn("Executor service was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Executor service did not terminate in the specified time {} sec. Exception: {}", SHUTDOWN_TIME,
        e.getMessage());
      List<Runnable> droppedTasks = leaderActionsExecutorService.shutdownNow();
      LOG.warn("Executor service was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
    }
  }

  @VisibleForTesting
  public static void scheduleClearDanglingScratchDir(HiveConf hiveConf, int initialWaitInSec) {
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_CLEAR_DANGLING_SCRATCH_DIR)) {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder()
          .namingPattern("cleardanglingscratchdir-%d")
          .daemon(true)
          .build());
      executor.scheduleAtFixedRate(new ClearDanglingScratchDir(false, false, false,
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.SCRATCHDIR), hiveConf), initialWaitInSec,
          HiveConf.getTimeVar(hiveConf, ConfVars.HIVE_SERVER2_CLEAR_DANGLING_SCRATCH_DIR_INTERVAL,
          TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
  }

  public void startPrivilegeSynchronizer(HiveConf hiveConf) throws Exception {

    if (!HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_PRIVILEGE_SYNCHRONIZER)) {
      return;
    }
    PolicyProviderContainer policyContainer = new PolicyProviderContainer();
    HiveAuthorizer authorizer = SessionState.get().getAuthorizerV2();
    if (authorizer.getHivePolicyProvider() != null) {
      policyContainer.addAuthorizer(authorizer);
    }
    if (MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS) != null &&
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS).contains(
        "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener") &&
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.HIVE_AUTHORIZATION_MANAGER)!= null) {
      List<HiveMetastoreAuthorizationProvider> providers = HiveUtils.getMetaStoreAuthorizeProviderManagers(
          hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER, SessionState.get().getAuthenticator());
      for (HiveMetastoreAuthorizationProvider provider : providers) {
        if (provider.getHivePolicyProvider() != null) {
          policyContainer.addAuthorizationProvider(provider);
        }
      }
    }

    if (policyContainer.size() > 0) {
      setUpZooKeeperAuth(hiveConf);
      zKClientForPrivSync = ZooKeeperHiveHelper.startZooKeeperClient(hiveConf, zooKeeperAclProvider, true);
      String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
      String path = ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
          + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "leader";
      LeaderLatch privilegeSynchronizerLatch = new LeaderLatch(zKClientForPrivSync, path);
      privilegeSynchronizerLatch.start();
      LOG.info("Find " + policyContainer.size() + " policy to synchronize, start PrivilegeSynchronizer");
      Thread privilegeSynchronizerThread = new Thread(
          new PrivilegeSynchronizer(privilegeSynchronizerLatch, policyContainer, hiveConf), "PrivilegeSynchronizer");
      privilegeSynchronizerThread.setDaemon(true);
      privilegeSynchronizerThread.start();
    } else {
      LOG.warn(
          "No policy provider found, skip creating PrivilegeSynchronizer");
    }
  }

  private static void startHiveServer2() throws Throwable {
    long attempts = 0, maxAttempts = 1;
    while (true) {
      LOG.info("Starting HiveServer2");
      HiveConf hiveConf = new HiveConf();
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      long retrySleepIntervalMs = hiveConf
          .getTimeVar(ConfVars.HIVE_SERVER2_SLEEP_INTERVAL_BETWEEN_START_ATTEMPTS,
              TimeUnit.MILLISECONDS);
      HiveServer2 server = null;
      try {
        // Cleanup the scratch dir before starting
        ServerUtils.cleanUpScratchDir(hiveConf);
        // Schedule task to cleanup dangling scratch dir periodically,
        // initial wait for a random time between 0-10 min to
        // avoid initial spike when using multiple HS2
        scheduleClearDanglingScratchDir(hiveConf, new Random().nextInt(600));

        server = new HiveServer2();
        server.init(hiveConf);
        server.start();

        try {
          JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(hiveConf);
          pauseMonitor.start();
        } catch (Throwable t) {
          LOG.warn("Could not initiate the JvmPauseMonitor thread." + " GCs and Pauses may not be " +
            "warned upon.", t);
        }

        break;
      } catch (Throwable throwable) {
        if (server != null) {
          try {
            server.stop();
          } catch (Throwable t) {
            LOG.info("Exception caught when calling stop of HiveServer2 before retrying start", t);
          } finally {
            server = null;
          }
        }
        if (++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          LOG.warn("Error starting HiveServer2 on attempt " + attempts
              + ", will retry in " + retrySleepIntervalMs + "ms", throwable);
          try {
            Thread.sleep(retrySleepIntervalMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  /**
   * Remove all znodes corresponding to the given version number from ZooKeeper
   *
   * @param versionNumber
   * @throws Exception
   */
  static void deleteServerInstancesFromZooKeeper(String versionNumber) throws Exception {
    HiveConf hiveConf = new HiveConf();
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    int baseSleepTime = (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    CuratorFramework zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    zooKeeperClient.start();
    List<String> znodePaths =
        zooKeeperClient.getChildren().forPath(
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
    List<String> znodePathsUpdated;
    // Now for each path that is for the given versionNumber, delete the znode from ZooKeeper
    for (int i = 0; i < znodePaths.size(); i++) {
      String znodePath = znodePaths.get(i);
      deleteSignal = new CountDownLatch(1);
      if (znodePath.contains("version=" + versionNumber + ";")) {
        String fullZnodePath =
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
                + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + znodePath;
        LOG.warn("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        System.out.println("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        zooKeeperClient.delete().guaranteed().inBackground(new DeleteCallBack())
            .forPath(fullZnodePath);
        // Wait for the delete to complete
        deleteSignal.await();
        // Get the updated path list
        znodePathsUpdated =
            zooKeeperClient.getChildren().forPath(
                ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
        // Gives a list of any new paths that may have been created to maintain the persistent ephemeral node
        znodePathsUpdated.removeAll(znodePaths);
        // Add the new paths to the znodes list. We'll try for their removal as well.
        znodePaths.addAll(znodePathsUpdated);
      }
    }
    zooKeeperClient.close();
  }

  private static class DeleteCallBack implements BackgroundCallback {
    @Override
    public void processResult(CuratorFramework zooKeeperClient, CuratorEvent event)
        throws Exception {
      if (event.getType() == CuratorEventType.DELETE) {
        deleteSignal.countDown();
      }
    }
  }

  public static void main(String[] args) {
    HiveConf.setLoadHiveServer2Config(true);
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
      ServerOptionsProcessorResponse oprocResponse = oproc.parse(args);

      // NOTE: It is critical to do this here so that log4j is reinitialized
      // before any of the other core hive classes are loaded
      String initLog4jMessage = LogUtils.initHiveLog4j();
      LOG.debug(initLog4jMessage);
      HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);

      // Logger debug message from "oproc" after log4j initialize properly
      LOG.debug(oproc.getDebugMessage().toString());

      // Call the executor which will execute the appropriate command based on the parsed options
      oprocResponse.getServerOptionsExecutor().execute();
    } catch (LogInitializationException e) {
      LOG.error("Error initializing log: " + e.getMessage(), e);
      System.exit(-1);
    }
  }

  /**
   * ServerOptionsProcessor.
   * Process arguments given to HiveServer2 (-hiveconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  static class ServerOptionsProcessor {
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    private final String serverName;
    private final StringBuilder debugMessage = new StringBuilder();

    @SuppressWarnings("static-access")
    ServerOptionsProcessor(String serverName) {
      this.serverName = serverName;
      // -hiveconf x=y
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("hiveconf")
          .withDescription("Use value for given property")
          .create());
      // -deregister <versionNumber>
      options.addOption(OptionBuilder
          .hasArgs(1)
          .withArgName("versionNumber")
          .withLongOpt("deregister")
          .withDescription("Deregister all instances of given version from dynamic service discovery")
          .create());
      // --listHAPeers
      options.addOption(OptionBuilder
        .hasArgs(0)
        .withLongOpt("listHAPeers")
        .withDescription("List all HS2 instances when running in Active Passive HA mode")
        .create());
      // --failover <workerIdentity>
      options.addOption(OptionBuilder
        .hasArgs(1)
        .withArgName("workerIdentity")
        .withLongOpt("failover")
        .withDescription("Manually failover Active HS2 instance to passive standby mode")
        .create());
      options.addOption(new Option("H", "help", false, "Print help information"));
    }

    ServerOptionsProcessorResponse parse(String[] argv) {
      try {
        commandLine = new GnuParser().parse(options, argv);
        // Process --hiveconf
        // Get hiveconf param values and set the System property values
        Properties confProps = commandLine.getOptionProperties("hiveconf");
        for (String propKey : confProps.stringPropertyNames()) {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
          if (propKey.equalsIgnoreCase("hive.root.logger")) {
            CommonCliOptions.splitAndSetLogger(propKey, confProps);
          } else {
            System.setProperty(propKey, confProps.getProperty(propKey));
          }
        }

        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options));
        }

        // Process --deregister
        if (commandLine.hasOption("deregister")) {
          return new ServerOptionsProcessorResponse(new DeregisterOptionExecutor(
              commandLine.getOptionValue("deregister")));
        }

        // Process --listHAPeers
        if (commandLine.hasOption("listHAPeers")) {
          return new ServerOptionsProcessorResponse(new ListHAPeersExecutor());
        }

        // Process --failover
        if (commandLine.hasOption("failover")) {
          return new ServerOptionsProcessorResponse(new FailoverHS2InstanceExecutor(
            commandLine.getOptionValue("failover")
          ));
        }
      } catch (ParseException e) {
        // Error out & exit - we were not able to parse the args successfully
        System.err.println("Error starting HiveServer2 with given arguments: ");
        System.err.println(e.getMessage());
        System.exit(-1);
      }
      // Default executor, when no option is specified
      return new ServerOptionsProcessorResponse(new StartOptionExecutor());
    }

    StringBuilder getDebugMessage() {
      return debugMessage;
    }
  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  static class ServerOptionsProcessorResponse {
    private final ServerOptionsExecutor serverOptionsExecutor;

    ServerOptionsProcessorResponse(ServerOptionsExecutor serverOptionsExecutor) {
      this.serverOptionsExecutor = serverOptionsExecutor;
    }

    ServerOptionsExecutor getServerOptionsExecutor() {
      return serverOptionsExecutor;
    }
  }

  /**
   * The executor interface for running the appropriate HiveServer2 command based on parsed options
   */
  static interface ServerOptionsExecutor {
    public void execute();
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  static class HelpOptionExecutor implements ServerOptionsExecutor {
    private final Options options;
    private final String serverName;

    HelpOptionExecutor(String serverName, Options options) {
      this.options = options;
      this.serverName = serverName;
    }

    @Override
    public void execute() {
      new HelpFormatter().printHelp(serverName, options);
      System.exit(0);
    }
  }

  /**
   * StartOptionExecutor: starts HiveServer2.
   * This is the default executor, when no option is specified.
   */
  static class StartOptionExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        startHiveServer2();
      } catch (Throwable t) {
        LOG.error("Error starting HiveServer2", t);
        System.exit(-1);
      }
    }
  }

  /**
   * DeregisterOptionExecutor: executes the --deregister option by deregistering all HiveServer2
   * instances from ZooKeeper of a specific version.
   */
  static class DeregisterOptionExecutor implements ServerOptionsExecutor {
    private final String versionNumber;

    DeregisterOptionExecutor(String versionNumber) {
      this.versionNumber = versionNumber;
    }

    @Override
    public void execute() {
      try {
        deleteServerInstancesFromZooKeeper(versionNumber);
      } catch (Exception e) {
        LOG.error("Error deregistering HiveServer2 instances for version: " + versionNumber
            + " from ZooKeeper", e);
        System.out.println("Error deregistering HiveServer2 instances for version: " + versionNumber
            + " from ZooKeeper." + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }

  /**
   * Handler for --failover <workerIdentity> command. The way failover works is,
   * - the client gets <workerIdentity> from user input
   * - the client uses HS2 HA registry to get list of HS2 instances and finds the one that matches <workerIdentity>
   * - if there is a match, client makes sure the instance is a leader (only leader can failover)
   * - if the matched instance is a leader, its web endpoint is obtained from service record then http DELETE method
   *   is invoked on /leader endpoint (Yes. Manual failover requires web UI to be enabled)
   * - the webpoint checks if admin ACLs are set, if so will close and restart the leader latch triggering a failover
   */
  static class FailoverHS2InstanceExecutor implements ServerOptionsExecutor {
    private final String workerIdentity;

    FailoverHS2InstanceExecutor(String workerIdentity) {
      this.workerIdentity = workerIdentity;
    }

    @Override
    public void execute() {
      try {
        HiveConf hiveConf = new HiveConf();
        HS2ActivePassiveHARegistry haRegistry = HS2ActivePassiveHARegistryClient.getClient(hiveConf);
        Collection<HiveServer2Instance> hs2Instances = haRegistry.getAll();
        // no HS2 instances are running
        if (hs2Instances.isEmpty()) {
          LOG.error("No HiveServer2 instances are running in HA mode");
          System.err.println("No HiveServer2 instances are running in HA mode");
          System.exit(-1);
        }
        HiveServer2Instance targetInstance = null;
        for (HiveServer2Instance instance : hs2Instances) {
          if (instance.getWorkerIdentity().equals(workerIdentity)) {
            targetInstance = instance;
            break;
          }
        }
        // no match for workerIdentity
        if (targetInstance == null) {
          LOG.error("Cannot find any HiveServer2 instance with workerIdentity: " + workerIdentity);
          System.err.println("Cannot find any HiveServer2 instance with workerIdentity: " + workerIdentity);
          System.exit(-1);
        }
        // only one HS2 instance available (cannot failover)
        if (hs2Instances.size() == 1) {
          LOG.error("Only one HiveServer2 instance running in thefail cluster. Cannot failover: " + workerIdentity);
          System.err.println("Only one HiveServer2 instance running in the cluster. Cannot failover: " + workerIdentity);
          System.exit(-1);
        }
        // matched HS2 instance is not leader
        if (!targetInstance.isLeader()) {
          LOG.error("HiveServer2 instance (workerIdentity: " + workerIdentity + ") is not a leader. Cannot failover");
          System.err.println("HiveServer2 instance (workerIdentity: " + workerIdentity + ") is not a leader. Cannot failover");
          System.exit(-1);
        }

        String webPort = targetInstance.getProperties().get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname);
        // web port cannot be obtained
        if (StringUtils.isEmpty(webPort)) {
          LOG.error("Unable to determine web port for instance: " + workerIdentity);
          System.err.println("Unable to determine web port for instance: " + workerIdentity);
          System.exit(-1);
        }

        // invoke DELETE /leader endpoint for failover
        String webEndpoint = "http://" + targetInstance.getHost() + ":" + webPort + "/leader";
        HttpDelete httpDelete = new HttpDelete(webEndpoint);
        CloseableHttpResponse httpResponse = null;
        try (
          CloseableHttpClient client = HttpClients.createDefault();
        ) {
          int statusCode = -1;
          String response = "Response unavailable";
          httpResponse = client.execute(httpDelete);
          if (httpResponse != null) {
            StatusLine statusLine = httpResponse.getStatusLine();
            if (statusLine != null) {
              response = httpResponse.getStatusLine().getReasonPhrase();
              statusCode = httpResponse.getStatusLine().getStatusCode();

              if (statusCode == 200) {
                System.out.println(EntityUtils.toString(httpResponse.getEntity()));
              }
            }
          }

          if (statusCode != 200) {
            // Failover didn't succeed - log error and exit
            LOG.error("Unable to failover HiveServer2 instance: " + workerIdentity +
                ". status code: " + statusCode + "error: " + response);
            System.err.println("Unable to failover HiveServer2 instance: " + workerIdentity +
                ". status code: " + statusCode + " error: " + response);
            System.exit(-1);
          }
        } finally {
          if (httpResponse != null) {
            httpResponse.close();
          }
        }
      } catch (IOException e) {
        LOG.error("Error listing HiveServer2 HA instances from ZooKeeper", e);
        System.err.println("Error listing HiveServer2 HA instances from ZooKeeper" + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }

  static class ListHAPeersExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        HiveConf hiveConf = new HiveConf();
        HS2ActivePassiveHARegistry haRegistry = HS2ActivePassiveHARegistryClient.getClient(hiveConf);
        HS2Peers.HS2Instances hs2Instances = new HS2Peers.HS2Instances(haRegistry.getAll());
        String jsonOut = hs2Instances.toJson();
        System.out.println(jsonOut);
      } catch (IOException e) {
        LOG.error("Error listing HiveServer2 HA instances from ZooKeeper", e);
        System.err.println("Error listing HiveServer2 HA instances from ZooKeeper" + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }

  private void invokeApplicationConnectionWatcher() {
    ApplicationConnectionWatcher watcher = new ApplicationConnectionWatcher();
    watcher.process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged,
            Watcher.Event.KeeperState.SyncConnected, ""));
  }

  private void invokeLeaderWatcher() {
    LeaderWatcher leaderWatcher = new LeaderWatcher();
    leaderWatcher.process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged,
            Watcher.Event.KeeperState.SyncConnected, ""));
  }

  private abstract class ApplicationWatcher implements CuratorWatcher {
    private static final String appIdPath = MR3ZooKeeperUtils.APP_ID_PATH;
    private static final String appIdLockPath = MR3ZooKeeperUtils.APP_ID_LOCK_PATH;
    private static final String appIdCheckRequestPath = MR3ZooKeeperUtils.APP_ID_CHECK_REQUEST_PATH;

    private InterProcessMutex appIdLock;
    private String namespace;

    ApplicationWatcher() {
      this.namespace = "/" + HiveServer2.this.getHiveConf().getVar(HiveConf.ConfVars.MR3_ZOOKEEPER_APPID_NAMESPACE);
      this.appIdLock = new InterProcessMutex(HiveServer2.this.zooKeeperClient, this.namespace + appIdLockPath);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      HiveServer2.this.watcherThreadExecutor.execute(() -> this.run(watchedEvent));
    }

    public abstract void run(WatchedEvent watchedEvent);

    protected void lockAppId() throws Exception {
      appIdLock.acquire();
    }

    protected void unlockAppId() throws Exception {
      if (appIdLock.isAcquiredInThisProcess()) {
        appIdLock.release();
      }
    }

    protected String readAppId() throws Exception {
      if (!appIdLock.isAcquiredInThisProcess()) {
        throw new RuntimeException("appIdLock is not acquired before reading appId");
      }
      return new String(HiveServer2.this.zooKeeperClient.getData().forPath(namespace + appIdPath));
    }

    protected void updateAppId(String newAppId) throws Exception {
      if (!appIdLock.isAcquiredInThisProcess()) {
        throw new RuntimeException("appIdLock is not acquired before updating appId");
      }
      if (HiveServer2.this.zooKeeperClient.checkExists().forPath(namespace + appIdPath) == null) {
        HiveServer2.this.zooKeeperClient.create().forPath(namespace + appIdPath, newAppId.getBytes());
      } else {
        HiveServer2.this.zooKeeperClient.setData().forPath(namespace + appIdPath, newAppId.getBytes());
      }
    }

    protected void registerWatcher(CuratorWatcher watcher, boolean isLeaderWatcher) throws Exception {
      if (isLeaderWatcher) {
        HiveServer2.this.zooKeeperClient.checkExists().usingWatcher(watcher).forPath(namespace + appIdCheckRequestPath);
      } else {
        HiveServer2.this.zooKeeperClient.checkExists().usingWatcher(watcher).forPath(namespace + appIdPath);
      }
    }

    // 1. return a non-empty string if readAppId() succeeds
    // 2. return "" if readAppId() reads nothing
    // 3. return null if ZooKeeper operation fails
    // 4. raise InterruptedException if interrupted
    // potentially creates an infinite loop if releaseLock == true
    protected String getSharedAppIdStr(boolean releaseLock) throws InterruptedException {
      String sharedAppIdStr;

      try {
        lockAppId();
        sharedAppIdStr = readAppId();
      } catch (KeeperException.NoNodeException ex) {
        sharedAppIdStr = "";
      } catch (InterruptedException ie) {
        throw new InterruptedException("Interrupted while reading ApplicationId");
      } catch (Exception ex) {
        LOG.error("Failed to connect to ZooKeeper while trying to read ApplicationId", ex);
        return null;
      } finally {
        if (releaseLock) {
          releaseAppIdLock();
        }
      }

      return sharedAppIdStr;
    }

    // potentially creates an infinite loop
    protected void releaseAppIdLock() throws InterruptedException {
      boolean unlockedAppId = false;
      while (!unlockedAppId) {
        try {
          unlockAppId();
          unlockedAppId = true;
        } catch (InterruptedException ie) {
          throw new InterruptedException("Interrupted while releasing lock for ApplicationId");
        } catch (Exception ex) {
          LOG.warn("Failed to release lock for ApplicationId, retrying in 10 seconds", ex);
          Thread.sleep(10000L);
        }
      }
    }

    // return true if successful
    // return false if interrupted
    // potentially creates an infinite loop
    protected boolean registerNextWatcher(boolean isLeaderWatcher) {
      boolean registeredNewWatcher = false;
      while (!registeredNewWatcher) {
        try {
          registerWatcher(this, isLeaderWatcher);
          LOG.info("New ApplicationConnectionWatcher registered");
          registeredNewWatcher = true;
        } catch (InterruptedException ie) {
          LOG.error("Interrupted while registering ApplicationConnectionWatcher, giving up");
          return false;
        } catch (Exception ex) {
          LOG.warn("Failed to register ApplicationConnectionWatcher, retrying in 10 seconds", ex);
          try {
            Thread.sleep(10000L);
            // TODO: in the case of isLeaderWatcher == true, we could give up after a certain number of
            // retries by calling HiveServer.this.stop()
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while registering ApplicationConnectionWatcher, giving up");
            return false;
          }
        }
      }
      LOG.info("Registered the next Watcher: isLeaderWatcher = " + isLeaderWatcher);
      return true;
    }
  }

  private class ApplicationConnectionWatcher extends ApplicationWatcher {
    private Logger LOG = LoggerFactory.getLogger(ApplicationConnectionWatcher.class);

    ApplicationConnectionWatcher() {
      super();
    }

    public void run(WatchedEvent watchedEvent) {
      LOG.info("ApplicationConnectionWatcher triggered from " + watchedEvent.getPath());

      SessionState.setCurrentSessionState(parentSession);

      if (!registerNextWatcher(false)) {
        return;
      }
      // now we have the next ApplicationConnectionWatcher running

      String sharedAppIdStr;
      try {
        sharedAppIdStr = getSharedAppIdStr(true);
      } catch (InterruptedException ie) {
        LOG.error("ApplicationConnectionWatcher interrupted", ie);
        return;
      }
      if (sharedAppIdStr == null) {
        return;
      }
      if (sharedAppIdStr.isEmpty()) {
        // called in the first ApplicationConnectionWatcher of the first HiveServer2
        return;
      }

      try {
        LOG.info("Setting active Application: " + sharedAppIdStr);
        MR3SessionManagerImpl.getInstance().setActiveApplication(sharedAppIdStr);
      } catch (HiveException ex) {
        LOG.info("Error in setting active Application ", ex);
        // no need to take further action because Beeline will keep complaining about connecting to the
        // current MR3Session, which will in turn trigger ApplicationConnectionWatcher
      }
    }
  }

  private class LeaderWatcher extends ApplicationWatcher {
    private Logger LOG = LoggerFactory.getLogger(LeaderWatcher.class);

    LeaderWatcher() {
      super();
    }

    public void run(WatchedEvent watchedEvent) {
      LOG.info("LeaderWatcher triggered from " + watchedEvent.getPath());

      SessionState.setCurrentSessionState(parentSession);

      if (!HiveServer2.this.isLeader())
        return;

      boolean stopHiveServer2 = false;
      boolean tryReleaseLock = true;
      try {
        String sharedAppIdStr;
        try {
          sharedAppIdStr = getSharedAppIdStr(false);
        } catch (InterruptedException ie) {
          LOG.error("LeaderWatcher interrupted", ie);
          return;
        }
        if (sharedAppIdStr == null) {
          // take no action because we only have register the next Watcher
        } else {
          String finalAppIdStr = null;
          boolean createdNewApplication = false;
          try {
            if (sharedAppIdStr.isEmpty()) {
              finalAppIdStr = createNewApplication();
              createdNewApplication = true;
            } else {
              if (MR3SessionManagerImpl.getInstance().checkIfValidApplication(sharedAppIdStr)) {
                finalAppIdStr = sharedAppIdStr;
                createdNewApplication = false;  // unnecessary
              } else  {
                MR3SessionManagerImpl.getInstance().closeApplication(sharedAppIdStr);
                LOG.info("closed Application " + sharedAppIdStr);
                finalAppIdStr = createNewApplication();
                createdNewApplication = true;
              }
            }
            updateAppId(finalAppIdStr);
          } catch (InterruptedException ie) {
            LOG.error("LeaderWatcher interrupted", ie);
            return;
          } catch (HiveException e) {
            LOG.error("Failed to create MR3 Application, killing HiveServer2", e);
            stopHiveServer2 = true;
          } catch (Exception e) {
            // MR3SessionManager worked okay, but ZooKeeper failed somehow
            if (createdNewApplication) {
              assert finalAppIdStr != null;
              MR3SessionManagerImpl.getInstance().closeApplication(finalAppIdStr);
            }
            stopHiveServer2 = true;
            tryReleaseLock = false;   // because trying to release lock is likely to end up with an infinite loop in releaseAppIdLock()
          }
        }
      } finally {
        if (tryReleaseLock) {
          try {
            releaseAppIdLock();
          } catch (InterruptedException ie) {
            LOG.error("LeaderWatcher interrupted", ie);
            return;
          }
        }
      }

      if (stopHiveServer2)  {
        HiveServer2.this.closeHiveSessions();
        HiveServer2.this.stop();
      } else {
        registerNextWatcher(true);
      }
    }
  }

  private String createNewApplication() throws HiveException {
    String newAppIdStr = MR3SessionManagerImpl.getInstance().createNewApplication();
    LOG.info("created new Application " + newAppIdStr);
    return newAppIdStr;
  }
}

package drv.cassdriver;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PreDestroy;

import cwdrg.lg.Lg;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

import cwdrg.util.json.JSONUtil;

public class DriverWrapper {
    private static final Lg log = new Lg(LoggerFactory.getLogger(DriverWrapper.class));

    private static final String HEALTHCHECK_KEYSPACE = "system";
    private static final String HEALTHCHECK_CQL = "select keyspace_name from schema_keyspaces limit 1";
    private static final String[] HEALTHCHECK_CQL_ARGS = {};
    private static final ConsistencyLevel HEALTHCHECK_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
    private static final Statement HEALTHCHECK_STATEMENT = new SimpleStatement(HEALTHCHECK_CQL).setConsistencyLevel(HEALTHCHECK_CONSISTENCY);

    private static final long DEFAULT_RECONNECTION_DELAY_MILLIS = 60000;
    private static final long DEFAULT_QUERY_TIMEOUT_MILLIS = 60000;

    private static final long CLOSE_WAIT_MILLIS = 5000;

    private long constantTimeReconnectionDelayMilliseconds = DEFAULT_RECONNECTION_DELAY_MILLIS;

    private List<String> clusterContactNodes;

    private Integer clusterPort;

    private Cluster cluster;

    private Session clusterSession;

    private boolean autoStart = false;

    private volatile boolean destroyed = false;

    private long queryTimeoutMillis = DEFAULT_QUERY_TIMEOUT_MILLIS;

    private boolean enableCompression = true;

    private boolean useConstantTimeReconnectionPolicy = true;

    private boolean useDowngradingConsistencyRetryPolicy = false;

    private final Object initLock = new Object();
    private final Object sessionLock = new Object();

    /**
     * This must be volatile for double-check locking to work
     */
    private volatile boolean initialized = false;

    Map<String, PreparedStatement> stmtCache = new HashMap<>();


    public PreparedStatement cachedPrepare(String cql) {
        PreparedStatement stmt = stmtCache.get(cql);
        if (stmt == null) {
            stmt = getSession().prepare(cql);
            stmtCache.put(cql, stmt);
        }
        return stmt;
    }


    @PreDestroy
    protected void destroy() {
        destroyed = true;

        if (clusterSession != null) {
            handleClose(clusterSession.closeAsync());
        }
        if (cluster != null) {
            handleClose(cluster.closeAsync());
        }
    }


    private static void handleClose(CloseFuture closeFuture) {
        try {
            closeFuture.get(CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.wrn(e.getMessage(), e);
        }

        if (!closeFuture.isDone()) {
            closeFuture.force();
        }
    }


    public boolean isHealthyQUORUM() {
        if (!initialized || destroyed) {
            return false;
        }

        try {
            Session session = getSession();

            // do a simple db query in QUORUM consistency
            St st = new St();
            st.setStmt(HEALTHCHECK_STATEMENT);
            st.setCql(HEALTHCHECK_CQL);
            st.setKeyspace(HEALTHCHECK_KEYSPACE);

            executeStatementSync(session, st);

            return true;
        } catch (Exception e) {
            log.wrn("healthcheck db statement failed: " + e.getMessage(), e);
            return false;
        }
    }


    public void initDataSources(Boolean strict) {
        if (autoStart && !initialized) {
            synchronized (initLock) {
                checkNotShutDown();
                if (!initialized) {
                    try {
                        doInitDataSources();
                        initialized = true;
                    } catch (NoHostAvailableException e) {
                        log.err("initDataSourceFailed: " + e.getMessage() + "; not stopping context initialization; hopefully DB will be available later", e);
                        if (strict) {
                            throw e;
                        }
                    }
                }
            }
        }
    }


    private void doInitDataSources() {
        Cluster.Builder builder = Cluster.builder();
        // port default is 9042
        if (clusterPort != null) {
            builder = builder.withPort(clusterPort);
        }

        builder.addContactPoints(clusterContactNodes.toArray(new String[clusterContactNodes.size()]));

        if (enableCompression) {
            log.inf("CASSANDRA JAVA-DRIVER INIT: enable compression", null);
            builder = builder.withCompression(ProtocolOptions.Compression.LZ4);
        }

        if (useConstantTimeReconnectionPolicy) {
            log.inf("CASSANDRA JAVA-DRIVER INIT: constant reconnection policy: millis: {}", null, constantTimeReconnectionDelayMilliseconds);
            builder.withReconnectionPolicy(new ConstantReconnectionPolicy(constantTimeReconnectionDelayMilliseconds));
        }

        if (useDowngradingConsistencyRetryPolicy) {
            log.inf("CASSANDRA JAVA-DRIVER INIT: downgrading consistency retry policy",null);
            builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        }

        // TODO: SSL, loadBalancing, reconnectionPolicy, retryPolicy, on a per-keyspace basis? such as system /
        // logging / tracing gets different policies?

        // build cluster
        cluster = builder.build();
        clusterSession = cluster.connect();

        Metadata metadata = cluster.getMetadata();
        log.inf("CASSANDRA JAVA-DRIVER INIT: Connected to cluster: {}\n", null, metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            log.inf("CASSANDRA JAVA-DRIVER INIT: Datacenter: {}; Host: {}; Rack: {}; Status: {}\n", null, host.getDatacenter(), host.getAddress(), host.getRack(), host.isUp() ? "up" : "down");
        }
    }


    public Session getSession() {
        initDataSources(false);
        if (!initialized) {
            throw new IllegalStateException("database connection not initialized");
        }

        synchronized (sessionLock) {
            checkNotShutDown();

            if (clusterSession == null || clusterSession.isClosed()) {
                clusterSession = cluster.connect();
            }
            return clusterSession;
        }
    }


    private void checkNotShutDown() {
        if (destroyed) {
            throw new IllegalStateException("application is shutting down");
        }
    }


    public void executeUpdateCQLs(final String keyspace, final CharSequence[] cqls, final String consistency) {
        Session session = getSession();

        for (CharSequence cqlcs : cqls) {
            log.dbg(log.d() ? "UPDATE CQL: {} {} {}\n{}" : "", null, keyspace, consistency, null, cqlcs);
            SimpleStatement simplestmt = new SimpleStatement(cqlcs.toString());
            simplestmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));

            St stmt = new St();
            stmt.setCql(cqlcs.toString());
            stmt.setStmt(simplestmt);
            stmt.setKeyspace(keyspace);

            executeStatementAsync(session, stmt);
        }
    }


    public Statement prepare(final String keyspace, final String cql, final Object[] prepArgsIn, final String consistency, Long usingTimestamp) {
        if (prepArgsIn != null && prepArgsIn.length != 0) {
            typeConvertPrepArgs(prepArgsIn);
            log.dbg(log.d() ? "PREPARE: {} {} {}\n" + JSONUtil.toJSON(prepArgsIn) + "\n{}" : "", log.t() ? new RuntimeException("STACKTRACE") : null, keyspace, consistency, usingTimestamp, cql);
            PreparedStatement prepStmt = cachedPrepare(cql);
            prepStmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
            // TODO: ?detect the metadata/types? use prepArgTypes?
            BoundStatement bindStmt = null;
            try { bindStmt = prepStmt.bind(prepArgsIn); } catch (Exception e) { log.err("stmt prp err for cql: " + cql + " args " + log.json(Arrays.asList(prepArgsIn)), e); }
            if (usingTimestamp != null) {
                bindStmt.setDefaultTimestamp(usingTimestamp);
            }
            return bindStmt;
        } else {
            SimpleStatement stmt = new SimpleStatement(cql);
            if (usingTimestamp != null) {
                stmt.setDefaultTimestamp(usingTimestamp);
            }
            stmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
            return stmt;
        }

    }

    public St prepareSt(final String keyspace, final String cql, final Object[] prepArgsIn, final String consistency, Long usingTimestamp) {
        St st = new St();
        st.setKeyspace(keyspace);
        st.setCql(cql);
        st.setCqlargs(prepArgsIn);
        if (prepArgsIn != null && prepArgsIn.length != 0) {
            typeConvertPrepArgs(prepArgsIn);
            log.dbg(log.d() ? "PREPARE: {} {} {}\n" + JSONUtil.toJSON(prepArgsIn) + "\n{}" : "", log.t() ? new RuntimeException("STACKTRACE") : null, keyspace, consistency, usingTimestamp, cql);
            PreparedStatement prepStmt = cachedPrepare(cql);
            prepStmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
            // TODO: ?detect the metadata/types? use prepArgTypes?
            BoundStatement bindStmt = null;
            try { bindStmt = prepStmt.bind(prepArgsIn); } catch (Exception e) { log.err("stmt prp err for cql: " + cql + " args " + log.json(Arrays.asList(prepArgsIn)), e); }
            if (usingTimestamp != null) {
                bindStmt.setDefaultTimestamp(usingTimestamp);
            }
            st.setStmt(bindStmt);
            return st;
        } else {
            SimpleStatement stmt = new SimpleStatement(cql);
            if (usingTimestamp != null) {
                stmt.setDefaultTimestamp(usingTimestamp);
            }
            stmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
            st.setStmt(stmt);
            return st;
        }

    }


    public ResultSet executeSyncStatementBatch(List<St> statements, final String consistency, final Long usingTimestamp) {
        BatchStatement batch = new BatchStatement();
        if (usingTimestamp != null) {
            batch.setDefaultTimestamp(usingTimestamp);
        }
        batch.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
        StringBuilder batchcql = log.d() ? new StringBuilder() : null;
        String keyspace = null;
        for (St stmt : statements) {
            if (keyspace == null) {
                keyspace = stmt.getKeyspace();
            } else {
                if (!StringUtils.equals(keyspace, stmt.getKeyspace())) {
                    log.wrn("batch statement keyspace mismatch: {} {}", null, keyspace, stmt.getKeyspace());
                }
            }
            batch.add(stmt.getStmt());
            batchcql.append(stmt.getCql()).append("\n");
            Object[] cqlargs = stmt.getCqlargs();
            if (cqlargs != null && cqlargs.length > 0) {
                batchcql.append("    [");
                for (Object cqlarg : cqlargs) {
                    batchcql.append("[").append(cqlarg == null ? null : cqlarg.toString()).append("]");
                }
                batchcql.append("]\n");
            }
        }
        log.dbg(log.d() ? "Batch\n " + batchcql : null, null);

        Session session = getSession();
        St batchstmt = new St();
        batchstmt.setStmt(batch);
        batchstmt.setKeyspace(keyspace);
        batchstmt.setCql(batchcql.toString());
        log.dbg(log.d() ? "EXEC ASYNC BATCH: {} {} {} \n{}" : "", null, keyspace, consistency, usingTimestamp, batchcql);

        return executeStatementAsync(session, batchstmt);
    }


    public ResultSet executeSyncStatement(St s) {
        Session session = getSession();
        return executeStatementSync(session, s);
    }


    public void executeDirectUpdate(final String keyspace, final String cql, final Object[] prepArgsIn, final String consistency, Long usingTimestamp) {
        log.dbg(log.d() ? "EXEC UPDATE: {} {} " + JSONUtil.serialize(prepArgsIn) + " {} {}" : "", null, keyspace, cql, consistency, usingTimestamp);
        Session session = getSession();

        Statement prepstmt = prepare(keyspace, cql, prepArgsIn, consistency, usingTimestamp);

        St stmt = new St();
        stmt.setStmt(prepstmt);
        stmt.setKeyspace(keyspace);
        stmt.setCql(cql);
        stmt.setCqlargs(prepArgsIn == null ? null : prepArgsIn.length == 0 ? null : prepArgsIn);

        executeStatementAsync(session, stmt);
    }


    public ResultSet executeStatementAsync(Session keyspaceSession, St stmt) {
        final long start = System.currentTimeMillis();

        final ResultSetFuture future = keyspaceSession.executeAsync(stmt.getStmt());

        try {
            final ResultSet rs = future.get(queryTimeoutMillis, TimeUnit.MILLISECONDS);

            long now = System.currentTimeMillis();

            log.dbg("execution ended after {} milliseconds; keyspace: {}; cql: {}; args: {}", null, now - start, stmt.getKeyspace(), stmt.getCql(), stmt.getCqlargs());
            log.dbg(log.d() ? " args JSON_: " + JSONUtil.serialize(stmt.getCqlargs()) : "", null);

            return rs;
        } catch (TimeoutException e) {
            future.cancel(true);
            final long now = System.currentTimeMillis();
            final String msg = String.format("execution timed out after %s milliseconds; keyspace: %s; cql: %s; args: %s", now - start, stmt.getKeyspace(), stmt.getCql(), stmt.getCqlargs());
            log.wrn(msg, e);
            logClusterStatus();
            throw new DrvTimeoutException(msg, e);
        } catch (InterruptedException | ExecutionException e) {
            final long now = System.currentTimeMillis();
            final String msg = String.format("execution failed after %s milliseconds; keyspace: %s; cql: %s; args: %s; message: %s", now - start, stmt.getKeyspace(), stmt.getCql(), stmt.getCqlargs(), e.getMessage());
            log.wrn(msg, e);
            logClusterStatus();
            throw new DrvStorageException(msg, e);
        }
    }


    public ResultSet executeStatementSync(Session keyspaceSession, St stmt) {
        final long start = System.currentTimeMillis();

        final ResultSet rs = keyspaceSession.execute(stmt.getStmt());

        long now = System.currentTimeMillis();

        log.dbg("execution ended after {} milliseconds; keyspace: {}; cql: {}; args: {}", null, now - start, stmt.getKeyspace(), stmt.getCql(), stmt.getCqlargs());
        log.dbg(log.d() ? " args JSON-: " + JSONUtil.serialize(stmt.getCqlargs()) : "", null);

        return rs;
    }


    public ResultSet initiateQuery(String keyspace, String cql, Object[] prepArgs, String consistency, Integer fetchSize, Integer fetchThreshold) {
        Session session = getSession();

        St st = new St();
        Statement stmt = prepare(keyspace, cql, prepArgs, consistency, null);
        st.setStmt(stmt);
        st.setCqlargs(prepArgs);
        st.setKeyspace(keyspace);
        ResultSet cassRS = executeStatementSync(session, st);
        return cassRS;
    }


    private void logClusterStatus() {
        try {
            for (Host host : cluster.getMetadata().getAllHosts()) {
                log.inf("{} is {}", null, host, host.isUp() ? "up" : "down");
            }
        } catch (RuntimeException re) {
            log.wrn("get cluster metadata failed: " + re.getMessage(), re);
        }
    }


    private static void typeConvertPrepArgs(Object[] args) {
        for (int i = 0; i < args.length; i++) {
            Object obj = args[i];
            if (obj instanceof byte[] || obj instanceof Byte[]) {
                obj = ByteBuffer.wrap((byte[]) obj);
            }
            if (obj instanceof java.util.Date) {
                obj = Instant.ofEpochMilli(((Date)obj).getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
            }
            args[i] = obj;
        }
    }


    public void executeQueryCQL(final String keyspace, String cql, Object[] prepArgs, final RowCallbackHandler rch, final String consistency) {
        Session session = getSession();

        // pass a facade of a RS to the rch?

        ResultSet cassRS = null; // NOSONAR this is not closeable

        log.dbg(log.d() ? "EXEC QUERY: {} {} {} \n{}" : "", null, keyspace, consistency, null, cql);

        St stmt = new St();
        stmt.setKeyspace(keyspace);
        stmt.setCql(cql);
        stmt.setCqlargs(prepArgs == null ? null : prepArgs.length == 0 ? null : prepArgs);
        if (prepArgs == null || prepArgs.length == 0) {
            stmt.setStmt(new SimpleStatement(cql).setConsistencyLevel(getConsistencyLevel(consistency)));
            cassRS = executeStatementAsync(session, stmt);
        } else {
            stmt.setStmt(new SimpleStatement(cql, prepArgs).setConsistencyLevel(getConsistencyLevel(consistency)));
            cassRS = executeStatementAsync(session, stmt);
        }

        int count = 0;

        for (Row curRow : cassRS) {
            ++count;
            try {
                rch.processRow(curRow);
            } catch (CQLInterruptQueryException e) {
                log.wrn("Query limit threshold reached for query '{}' returning partial search results", null, cql);
                break;
            } catch (Exception sqle) {
                throw new DrvRetrievalException("JAVADRIVER Query row processing error for cql: " + cql, sqle);
            }
        }

        log.dbg("query: {}; args: {}; count: {}", null, cql, prepArgs, count);

    }


    public static ConsistencyLevel getConsistencyLevel(String consistency) {
        return ConsistencyLevel.valueOf(consistency);
    }


    public void refreshConnection() {
        doInitDataSources();
    }


    public Set<String> getKeyspaces() {
        getSession();

        // need a new session for the keyspace (per java-driver documentation)
        List<KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
        Set<String> keyspaceNames = new HashSet<>(keyspaces.size());
        for (KeyspaceMetadata keyspace : keyspaces) {
            keyspaceNames.add(keyspace.getName());
        }

        log.dbg("getKeyspaces: return: {}", null, keyspaceNames);
        return keyspaceNames;
    }


    // ---- getter setters

    public void setClusterContactNodes(String clusterContactNodes) {
        String[] nodesArray = clusterContactNodes.split(",");

        this.clusterContactNodes = Arrays.asList(nodesArray);
    }


    public void setClusterPort(String clusterPort) {
        if (!"DEFAULT".equalsIgnoreCase(clusterPort)) {
            this.clusterPort = Integer.parseInt(clusterPort);
        }
    }


    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }


    public void setUseConstantTimeReconnectionPolicy(boolean useConstantTimeReconnectionPolicy) {
        this.useConstantTimeReconnectionPolicy = useConstantTimeReconnectionPolicy;
    }


    public void setUseDowngradingConsistencyRetryPolicy(boolean useDowngradingConsistencyRetryPolicy) {
        this.useDowngradingConsistencyRetryPolicy = useDowngradingConsistencyRetryPolicy;
    }


    public void setConstantTimeReconnectionDelayMilliseconds(long constantTimeReconnectionDelayMilliseconds) {
        this.constantTimeReconnectionDelayMilliseconds = constantTimeReconnectionDelayMilliseconds;
    }


    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }


    public void setQueryTimeoutMillis(long queryTimeoutMillis) {
        this.queryTimeoutMillis = queryTimeoutMillis;
    }
}

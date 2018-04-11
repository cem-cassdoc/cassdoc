package drv

import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.CloseFuture
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.Host
import com.datastax.driver.core.Metadata
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ProtocolOptions
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ResultSetFuture
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import javax.annotation.PreDestroy
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@CompileStatic
@Slf4j
class Drv {
    private static final String HEALTHCHECK_CQL = 'select keyspace_name from schema_keyspaces limit 1'
    private static final long DEFAULT_RECONNECTION_DELAY_MILLIS = 60000
    private static final long CLOSE_WAIT_MILLIS = 5000

    List<String> nodes = ['127.0.0.1']
    Integer port = null // default is 9042
    boolean autoStart = true
    String defaultConsistency = "LOCAL_QUORUM"
    String u
    String p


    Cluster.Builder builder

    private volatile boolean initialized = false
    private boolean destroyed = false
    private final Object initLock = new Object()

    private Cluster cluster = null
    private Session clusterSession = null
    protected Map<String, Session> keyspaceSessions = [:]
    protected Map<String, PreparedStatement> stmtCache = [:]

    // --- health checks

    long getHealthy() {
        if (!initialized || destroyed) {
            return 0
        }
        try {
            long time = System.currentTimeMillis()
            if (execSync(HEALTHCHECK_CQL, null).one() == null) {
                return 0
            }
            time = System.currentTimeMillis() - time
            return time == 0 ? 1 : time
        } catch (Exception e) {
            log.warn("healthcheck $defaultConsistency fail: ${e.message} ", e)
            return 0
        }
    }

    long getAllHealthy() {
        if (!initialized || destroyed) {
            return 0
        }
        try {
            long time = System.currentTimeMillis()
            if (execSync(new St(cql: HEALTHCHECK_CQL, consistency: 'ALL')).one() == null){
                return 0
            }
            time = System.currentTimeMillis() - time
            return time == 0 ? 1 : time
        } catch (Exception e) {
            log.warn("healthcheck-ALL fail: ${e.message} ", e)
            return 0
        }
    }

    List<Object[]> clusterStatus() {
        if (!initialized || destroyed) {
            return null
        }
        List<Object[]> status = []
        for (Host host: cluster.metadata.allHosts) {
            status.add([host.address,host.up] as Object[])
        }
        return status
    }

    Set<String> getKeyspaces() {
        if (!initialized || destroyed) {
            return null
        }
        Set<String> keyspaceNames = [] as Set
        cluster.metadata.keyspaces.each { keyspaceNames.add(it.name) }
        return keyspaceNames
    }

    // --- query/update execution

    // async: can use Futures.addCallback to the returned ResultSetFuture
    // https://www.datastax.com/dev/blog/java-driver-async-queries
    // ... groovy?
    ResultSetFuture execAsync(String cql, Object[] args) {
        St st = new St(cql: cql, args: args)
        execAsync(st)
    }

    ResultSetFuture execAsync(St st) {
        execAsync(getSession(st.keyspace),st)
    }

    ResultSetFuture execAsync(Session session, St st) {
        prepSt(st)
        long start = System.currentTimeMillis()
        ResultSetFuture rs = session.executeAsync(st.stmt)
        long stop = System.currentTimeMillis()
        log.debug("execAsync response: ${stop-start}ms")
        log.debug("  cql: ${st.cql ?: cqlFromStmt(st.stmt)}")
        log.debug("  args: ${st.args}")
        return rs
    }

    List<ResultSetFuture> asyncSpray(List<St> stList, String consistency, Long timestamp) {
        List<ResultSetFuture> results = []
        stList.each {
            if (consistency) { it.consistency = consistency }
            if (timestamp) { it.timestamp = timestamp }
            results.add(execAsync(it))
        }
        return results
    }

    ResultSet execSync(String cql, Object[] args) {
        St st = new St(cql: cql, args: args)
        execSync(st)
    }

    ResultSet execSync(St st) {
        execSync(getSession(st.keyspace),st)
    }

    ResultSet execSync(Session session, St st) {
        prepSt(st)
        long start = System.currentTimeMillis()
        ResultSet rs = session.execute(st.stmt)
        long stop = System.currentTimeMillis()
        log.debug("execSync response: ${stop-start}ms")
        log.debug("  cql: ${st.cql ?: cqlFromStmt(st.stmt)}")
        log.debug("  args: ${st.args}")
        return rs
    }

    List<Object[]> syncAll(String cql, Object[] args) {
        List<Object[]> results = []
        Iterator<Object[]> iter = syncIterator(cql, args)
        while (iter.hasNext()) {
            results.add(iter.next())
        }
        return results
    }

    List<Object[]> syncAll(St st) {
        List<Object[]> results = []
        Iterator<Object[]> iter = syncIterator(st)
        while (iter.hasNext()) {
            results.add(iter.next())
        }
        return results
    }

    Iterator<Object[]> syncIterator(String cql, Object[] args) {
        syncIterator(new St(cql:cql, args:args))
    }

    Iterator<Object[]> syncIterator(St st) {
        new RowArrayIterator(resultSet: execSync(st), fetchThreshold: st.fetchThreshold)
    }

    /**
     * By taking a cql statement and an interator of prep args for that statement, async
     * single-row queries (the cql must return only a single row, other rows will be ignored)
     * are "sprayed" using the driver's token awareness.
     *
     * The queries are executed in "batches" or "query sets". This defaults to 40 per set.
     *
     * TODO: efficiency testing.
     * TODO: v2: group tokens together
     *
     * @param cql           preppable statement
     * @param argsIterator  series of arguments to invoke the prepped cql statement with
     * @return              an iterator that returns rows
     */
    Iterator<Object[]> asyncSprayQuery(String cql, Iterator<Object[]> argsIterator) {
        asyncSprayQuery(new St(cql:cql,fetchSize: 40, fetchThreshold: 20), argsIterator)
    }

    Iterator<Object[]> asyncSprayQuery(St st, Iterator<Object[]> argsIterator) {
        return new AsyncQuerySetIterator(this, st, argsIterator)
    }

    // --- preparation and conversion

    PreparedStatement prepare(String cql) {
        PreparedStatement stmt = stmtCache[cql]
        if (!stmt) {
            stmt = session.prepare(cql)
            stmtCache[cql] = stmt
        }
        return stmt
    }

    void prepSt(St st) {
        if (!st.stmt) {
            st.stmt = makeStmt(st.cql, st.args, st.consistency, st.timestamp)
        } else {
            if (st.timestamp) {
                st.stmt.defaultTimestamp = st.timestamp
            }
            if (st.consistency) {
                st.stmt.consistencyLevel = ConsistencyLevel.valueOf(st.consistency)
            }
        }
        if (st.fetchSize) {
            st.stmt.fetchSize = st.fetchSize
        }

    }

    Statement makeStmt(String cql, Object[] prepArgs, String consistency, Long usingTimestamp) {
        try {
            Statement stmt = prepArgs ? prepare(cql).bind(typeConvertPrepArgs(prepArgs)) : new SimpleStatement(cql)
            stmt.consistencyLevel = ConsistencyLevel.valueOf(consistency ?: defaultConsistency)
            return stmt
        } catch (Exception e) {
            log.error("stmt prep: $cql $prepArgs", e)
            // TODO: log and throw with cql and prepArgs bound
            throw e
        }
    }

    static Object[] typeConvertPrepArgs(Object[] args) {
        for (int i = 0; i < args.length; i++) {
            Object obj = args[i]
            if (obj instanceof byte[] || obj instanceof Byte[]) {
                args[i] = ByteBuffer.wrap((byte[]) obj)
            } else if (obj instanceof java.util.Date) {
                args[i] = Instant.ofEpochMilli(((java.util.Date) obj).getTime()).atZone(ZoneId.systemDefault()).toLocalDate()
            }
        }
        return args
    }

    static String cqlFromStmt(Statement stmt) {
        if (stmt instanceof SimpleStatement) {
            return ((SimpleStatement)stmt).queryString
        }
        if (stmt instanceof BoundStatement) {
            return ((BoundStatement)stmt).preparedStatement().queryString
        }
        if (stmt instanceof BatchStatement) {
            StringBuilder sb = new StringBuilder()
            ((BatchStatement)stmt).statements.each {sb.append(cqlFromStmt(it))}
            return sb.toString()
        }
        return "Unknown statement type: ${stmt.class}"
    }

    static Object[] rowToArray(Row r) {
        ColumnDefinitions defs = r.columnDefinitions
        Object[] arr = new Object[defs.size()]
        for (int i=0; i < defs.size(); i++) {
            arr[i] = r.getObject(i)
        }
        return arr
    }

    // --- startup

    void initDataSources() {
        if (!initialized) {
            doInitDataSources()
        }
    }

    private void checkNotShutDown() {
        if (destroyed) {
            throw new IllegalStateException("application is shutting down");
        }
    }

    Session getSession() {
        getSession(null)
    }

    Session getSession(String keyspace) {
        if (!initialized) {
            if (autoStart) {
                initDataSources()
            } else {
                throw new IllegalStateException("driver not initialized")
            }
        }
        checkNotShutDown()
        if (keyspace == null) {
            return clusterSession
        }
        if (!keyspaceSessions.containsKey(keyspace)) {
            Session sess = cluster.connect(keyspace)
            keyspaceSessions[keyspace] = sess
            return sess
        } else {
            return keyspaceSessions[keyspace]
        }
    }

    private Cluster.Builder defaultPolicyBuilder() {
        Cluster.Builder builder = Cluster.builder()
        // default load balance is token-aware(dc-round-robin)
        log.info("DRV INIT: defaulting to LZ4 compression")
        builder = builder.withCompression(ProtocolOptions.Compression.LZ4)
        log.info("DRV INIT: defaulting to constant time reconnection policy")
        builder = builder.withReconnectionPolicy(new ConstantReconnectionPolicy(DEFAULT_RECONNECTION_DELAY_MILLIS))
        log.info("DRV INIT: defaulting to downgrading retry policy")
        builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
    }

    private void doInitDataSources() {
        synchronized (initLock) {
            checkNotShutDown();
            if (!initialized) {
                try {
                    Cluster.Builder builder = this.builder ?: defaultPolicyBuilder()
                    if (u || p) {
                        builder = builder.withCredentials(u,p)
                    }
                    log.info("DRV INIT: contact nodes: $nodes")
                    builder = builder.addContactPoints(nodes as String[])
                    if (port != null) {
                        log.info("DRV INIT: port: $port")
                        builder = builder.withPort(port)
                    }
                    // build cluster
                    cluster = builder.build()
                    clusterSession = cluster.connect()
                    Metadata metadata = cluster.getMetadata();
                    log.info("DRV: Connected to cluster: ${metadata.clusterName}")
                    for (Host host : metadata.getAllHosts()) {
                        log.info("DRV: Datacenter: ${host.datacenter}; Host: ${host.address}; Rack: ${host.rack}; Status: ${host.up ? 'up' : 'down'}")
                    }
                    initialized = true;
                } catch (NoHostAvailableException e) {
                    log.error("initDataSourceFailed: ${e.getMessage()}; not stopping context initialization", e)
                }
            }
        }
    }

    // --- shutdown
    @PreDestroy
    protected void destroy() {
        destroyed = true

        if (session != null) {
            handleClose(session.closeAsync())
        }
        if (cluster != null) {
            handleClose(cluster.closeAsync());
        }
    }

    private static void handleClose(CloseFuture closeFuture) {
        try {
            closeFuture.get(CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS)
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.warn(e.getMessage(), e)
        }

        if (!closeFuture.isDone()) {
            closeFuture.force()
        }
    }

    // --- accessors
    Cluster getCluster() { cluster }

    // --- builder methods
    Drv nodes(String nodesCSV) {
        nodes = nodesCSV?.split(',')
        return this
    }

    Drv nodes(List<String> nodeList) {
        nodes = nodeList
        return this
    }

    Drv port(Integer port) {
        this.port = port
        return this
    }

}

class RowArrayIterator implements Iterator<Object[]> {
    ResultSet resultSet
    Integer fetchThreshold

    @Override
    boolean hasNext() {
        !resultSet.exhausted
    }

    @Override
    Object[] next() {
        Row row = resultSet.one()
        if (fetchThreshold) {
            if (resultSet.getAvailableWithoutFetching() <= fetchThreshold && !resultSet.fullyFetched) {
                resultSet.fetchMoreResults()
            }
        }
        Drv.rowToArray(row)
    }
}

class St {
    String cql
    Object[] args
    Statement stmt // cql overrides stmt...
    String consistency
    Long timestamp
    String keyspace
    Integer fetchSize
    Integer fetchThreshold
}

// runs two query sets (in theory): one is being processed while the other is being retrieved
// ASSUMES SINGLE-ROW-RESULT QUERIES BEING TOKEN-AWARE SPRAY-CALLED
class AsyncQuerySetIterator implements Iterator<Object[]> {

    AsyncQuerySetIterator(Drv drv, St st, Iterator<Object[]> cqlArgs) {
        this.st = st
        this.cqlArgs = cqlArgs
        if (!st.fetchSize) {
            st.fetchSize = 40
        }
        currentQuerySet = new ResultSetFuture[st.fetchSize]
        nextQuerySet = new ResultSetFuture[st.fetchSize]
        initiateQuerySet(currentQuerySet)
        nextQuerySetRetrieved = false
    }

    boolean hasNext() {
        for (; currentQuerySetIndex < currentQuerySet.length; currentQuerySetIndex++) {
            if (currentQuerySet[currentQuerySetIndex] != null) {
                return true
            }
        }
        // current query set is exhausted
        if (nextQuerySetRetrieved) {
            ResultSetFuture[] temp = currentQuerySet
            currentQuerySet = nextQuerySet
            nextQuerySet = temp
            nextQuerySetRetrieved = false
            currentQuerySetIndex = 0
            return true
        }
        // completely exhausted, apparently
        return false
    }

    Object[] next() {
        for (; currentQuerySetIndex < currentQuerySet.length; currentQuerySetIndex++) {
            if (currentQuerySet[currentQuerySetIndex] != null) {
                if (!nextQuerySetRetrieved && currentQuerySetIndex > st.fetchSize.intdiv(2)) {
                    initiateQuerySet(nextQuerySet)
                }
                ResultSetFuture rsf = currentQuerySet[currentQuerySetIndex]
                currentQuerySet[currentQuerySetIndex] = null
                return rowToArray(rsf.get().one())
            }
        }
        // hasNext() should prevent this from happening.
    }

    private St st
    private Iterator<Object[]> cqlArgs
    private ResultSetFuture[] currentQuerySet
    private ResultSetFuture[] nextQuerySet
    private boolean nextQuerySetRetrieved
    private int currentQuerySetIndex
    private Drv drv

    private void initiateQuerySet(ResultSetFuture[] querySet) {
        int i = 0
        while (i < st.fetchSize && cqlArgs.hasNext()) {
            st.args = cqlArgs.next()
            querySet[i] = drv.execAsync(st)
            i++
        }
        if (i == 0) {
            nextQuerySetRetrieved = false
        } else {
            nextQuerySetRetrieved = true
        }
    }

}

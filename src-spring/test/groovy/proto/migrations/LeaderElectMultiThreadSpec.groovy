package proto.migrations

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.SimpleStatement
import drv.cassdriver.DriverWrapper
import drv.cassdriver.St
import spock.lang.Specification

import java.security.SecureRandom

// @Stepwise
class LeaderElectMultiThreadSpec extends Specification {

    static List<String> cassIPs = ["172.17.0.2", "172.17.0.3", "172.17.0.4"]
    static SecureRandom r = new SecureRandom()
    static String keyspace = "ticker"

    static List<String> setupKeyspace = [
            "CREATE KEYSPACE $keyspace WITH replication = {'class':'SimpleStrategy','replication_factor':3};"
    ]
    static List<String> setupMigrationSchema = [
            "CREATE TABLE IF NOT EXISTS ticker.migrations (name text, sha text, leader text, PRIMARY KEY (name));"
    ]

    static List<String> migrationCommands = [
            "CREATE TABLE second_schedules (second bigint, group text, name text, jobtype text, status text, PRIMARY KEY(second,group,name,jobtype);",
            "CREATE TABLE partitioned_second_schedules (second bigint, partition bigint, group text, name text, jobtype text, status text, PRIMARY KEY((second,partition),group,name,jobtype);",
            "CREATE TABLE scheduled (group text, second bigint, name text, jobtype text, status text, PRIMARY KEY(group,second,name,jobtype);",
            "ALTER TABLE second_schedules WITH compaction = { 'class':'LeveledCompactionStrategy','tombstone_compaction_interval':3600, 'tombstone_threshold':0.2, 'unchecked_tombstone_compaction':true } and GC_GRACE_SECONDS = 0;",
            "ALTER TABLE scheduled WITH compaction = { 'class':'LeveledCompactionStrategy','tombstone_compaction_interval':3600, 'tombstone_threshold':0.2, 'unchecked_tombstone_compaction':true } and GC_GRACE_SECONDS = 0;",
            "ALTER TABLE scheduled WITH GC_GRACE_SECONDS = 10800;",
            "ALTER TABLE second_schedules WITH GC_GRACE_SECONDS = 10800;",
            "ALTER TABLE scheduled ADD partition bigint;"
    ]

    static String clearLeader = "UPDATE ticker.migrations SET leader = null WHERE name = 'LEADERELECT'"

    static String leaderElect(long id) {
        "UPDATE ticker.migrations SET leader = '$id' WHERE name = 'LEADERELECT' IF leader = null"
    }

    void 'threaded test of create keyspace'() {
        when:
        DriverWrapper drv = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs.join(','))
        drv.executeDirectUpdate("", "DROP KEYSPACE ticker;", null, "ALL", null)

        List<Thread> threads = []
        20.times {
            threads.add(new Thread() {
                void run() {
                    DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])
                    try {
                        wrapper.executeDirectUpdate("", setupKeyspace[0], null, "ONE", null)
                        wrapper.executeDirectUpdate(keyspace, setupMigrationSchema[0], null, "ONE", null)
                        println "done ${currentThread().id}"
                    } catch (Exception e) {
                        e.printStackTrace()
                    }
                }
            }
            )
        }

        // start them all
        println "=======================STARTING THREADS"
        threads*.start()

        sleep(5000)
        println "agreement? " + drv.cluster.metadata.checkSchemaAgreement()

        then:
        1 == 1

        // creation keyspace with consistency ONE results in some exceptions, but it all seemed to work out
        // ... ALL consistency was much better.
    }

    void 'concurrent leader election'() {
        when:
        long time = new Date().time + 1000
        List<Thread> threads = []
        DriverWrapper drv = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs.join(','))
        drv.executeDirectUpdate("", clearLeader, null, "ALL", null)

        100.times {
            threads.add(new Thread() {
                void run() {
                    DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])
                    try {
                        println "leaderelect: ${leaderElect(currentThread().id)}"
                        SimpleStatement elect = new SimpleStatement(leaderElect(currentThread().id)).setConsistencyLevel(ConsistencyLevel.ALL)
                        while (new Date().time < time) {
                            sleep(2)
                        }
                        ResultSet rs = wrapper.executeSyncStatement(new St(stmt: elect, cql: leaderElect(currentThread().id), keyspace: keyspace))
                        boolean isLeader = rs.one().getBool(0)

                        println "done ${currentThread().id}, leader: $isLeader"
                    } catch (Exception e) {
                        e.printStackTrace()
                    }
                }
            }
            )
        }
        // start them all
        println "=======================STARTING THREADS"
        threads*.start()

        sleep(10000)

        String cql = "SELECT leader FROM ticker.migrations where name = 'LEADERELECT'"
        ResultSet rs = drv.executeSyncStatement(new St(stmt: new SimpleStatement(cql).setConsistencyLevel(ConsistencyLevel.QUORUM), cql: cql, keyspace: keyspace))
        println "leader: "+rs.one().getString(0)

        then:
        1 == 1

        // QUORUM seemed to work... total time was about 3 seconds for the 100 threads, all local
        // ALL was 4 seconds, maybe a bit slower...
        // I'm a bit paranoid of thread upper limit throttling...
    }


}


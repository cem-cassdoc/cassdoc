package proto.ticker

import cassdoc.springmvc.RESTFunctionalTestSpec
import com.datastax.driver.core.ResultSet
import drv.cassdriver.DriverWrapper
import drv.cassdriver.St
import spock.lang.Specification

import java.security.SecureRandom

class Proto extends Specification {

    static final int shardCount = 50
    static final int futureWindowSeconds = 12*3600

    static List<String> cassIPs = ["172.17.0.2", "172.17.0.3", "172.17.0.4"]
    static SecureRandom r = new SecureRandom()
    static String keyspace = "ticker"

    void DROP_KS_quietly(DriverWrapper wrapper) {
        try {
            wrapper.executeDirectUpdate(keyspace, "DROP KEYSPACE ticker", null, "ALL", null)
        } catch (Exception e) {
            println "no ks"
        }
    }
    String CREATE_KS = "CREATE KEYSPACE ticker WITH replication = {'class':'SimpleStrategy','replication_factor':3};"

    String CREATE_scheduleBySecond(String s) {
        """
        CREATE TABLE ticker.second_schedules${s ?: ''} (
            second bigint,
        
            group  varchar,
            name varchar,
            jobtype varchar,
        
            status varchar,
            runtime bigint,
        
            expression varchar,
            timezone varchar,
            data varchar,
        
            PRIMARY KEY (second, group, name, jobtype)
        );
        """
    }

    String CREATE_ScheduleByGroup(String s) {
        """    
        CREATE TABLE ticker.scheduled${s ?: ''} (
            group  varchar,
        
            second bigint,
            name varchar,
            jobtype varchar,
        
            status varchar,
            runtime bigint,
        
            expression varchar,
            timezone varchar,
            data varchar,
        
            PRIMARY KEY (group, second, name, jobtype)
        );"""
    }

    String alter1(String s) {
        "ALTER TABLE ticker.second_schedules${s ?: ''} WITH compaction = { 'class':'LeveledCompactionStrategy','tombstone_compaction_interval':3600, 'tombstone_threshold':0.2, 'unchecked_tombstone_compaction':true } and GC_GRACE_SECONDS = 0;"
    }

    String alter2(String s) {
        "ALTER TABLE ticker.scheduled${s ?: ''} WITH compaction = { 'class':'LeveledCompactionStrategy','tombstone_compaction_interval':3600, 'tombstone_threshold':0.2, 'unchecked_tombstone_compaction':true } and GC_GRACE_SECONDS = 0;"
    }

    String insert1(String s) {
        "INSERT INTO ticker.second_schedules${s ?: ''} (second,group,name,jobtype,data) VALUES (?,?,?,?,?);"
    }

    String insert2(String s) {
        "INSERT INTO ticker.scheduled${s ?: ''} (group,second,name,jobtype,data) VALUES (?,?,?,?,?);"
    }

    String ttlInsert1(String s) {
        "INSERT INTO ticker.second_schedules${s ?: ''} (second,group,name,jobtype,data) VALUES (?,?,?,?,?) USING TTL ? ;"
    }

    String ttlInsert2(String s) {
        "INSERT INTO ticker.scheduled${s ?: ''} (group,second,name,jobtype,data) VALUES (?,?,?,?,?) USING TTL ?;"
    }

    String select1(String s) {
        "SELECT group, name, jobtype, data FROM ticker.second_schedules${s ?: ''} WHERE second = ?"
    }

    List<Thread> schedulerList() {
        List<Thread> list = []
        20.times {
            list.add(new Thread() {
                void run() {
                    println "started scheduler ${currentThread().id}"
                    DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])
                    String name = UUID.randomUUID()
                    String group = "grp" + r.nextInt(4)
                    int i = 0
                    2000000.times {
                        int ttl = 3600 + r.nextInt(12 * 3600) // TTL in 1 hour to 12 hours from now
                        long second = new Date().time.intdiv(1000) + ttl
                        String randData = UUID.randomUUID()
                        St s1 = wrapper.prepareSt("ticker", ttlInsert1(), [second, group, name, 'jt', randData, ttl + 10] as Object[], "QUORUM", null)
                        St s2 = wrapper.prepareSt("ticker", ttlInsert2(), [group, second, name, 'jt', randData, ttl + 10] as Object[], "QUORUM", null)
                        wrapper.executeSyncStatement(s1)
                        wrapper.executeSyncStatement(s2)
                        i++
                        if (i % 1000 == 0) {
                            println "thread ${currentThread().id} at $i"
                        }
                    }
                }
            })
        }
        return list
    }

    List<Thread> workerList() {
        List<Thread> list = []
        4.times {
            list.add(new Thread() {
                void run() {
                    println "started worker ${currentThread().id}"
                    DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])
                    int i = 0
                    long lastsec = 0
                    while (true) {
                        long second = new Date().time.intdiv(1000)
                        if (second != lastsec) {
                            lastsec = second
                            long millis = new Date().time
                            St st = wrapper.prepareSt("ticker", select1(), [second] as Object[], "QUORUM", null)
                            ResultSet rs = wrapper.executeSyncStatement(st)
                            int size = rs.all().size()
                            long second2 = new Date().time
                            println "--->READER: for sec: $second ev: $size resp: ${second2 - millis}"
                            i++
                            if (i % 1000 == 0) {
                                println "thread ${currentThread().id} at $i"
                            }
                        }
                    }
                }
            })
        }
        return list
    }

    void createBaseSchema(DriverWrapper wrapper) {
        wrapper.executeDirectUpdate(keyspace, CREATE_KS, null, "ALL", null)
        wrapper.executeDirectUpdate(keyspace, CREATE_scheduleBySecond(), null, "ALL", null)
        wrapper.executeDirectUpdate(keyspace, CREATE_ScheduleByGroup(), null, "ALL", null)
        wrapper.executeDirectUpdate(keyspace, alter1(), null, "ALL", null)
        wrapper.executeDirectUpdate(keyspace, alter2(), null, "ALL", null)
    }

    long calculateShard(long epochMillis) {
        // I'll need to rejigger this for a bit of tolerance (say... ten seconds? need to think about this)
        // and 1-2 "old" shards that stick around... this will do for testing
        int millisPerShard = (futureWindowSeconds * 1000).intdiv(shardCount)
        int shard = epochMillis.intdiv(millisPerShard) % shardCount
    }

    List<Thread> shardedSchedulerList() {
        List<Thread> list = []
        20.times {
            list.add(new Thread() {
                void run() {
                    println "started scheduler ${currentThread().id}"
                    DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])
                    String name = UUID.randomUUID()
                    String group = "grp" + r.nextInt(4)
                    int i = 0
                    2000000.times {
                        int ttlSeconds = r.nextInt(futureWindowSeconds) // random time in the future (within window)
                        int ttlShard = calculateShard(new Date().time + ttlSeconds * 1000)
                        long secondKey = new Date().time.intdiv(1000) + ttlSeconds
                        String randData = UUID.randomUUID()
                        St s1 = wrapper.prepareSt("ticker", ttlInsert1("" + ttlShard), [secondKey, group, name, 'jt', randData, ttlSeconds + 10] as Object[], "QUORUM", null)
                        St s2 = wrapper.prepareSt("ticker", ttlInsert2("" + ttlShard), [group, secondKey, name, 'jt', randData, ttlSeconds + 10] as Object[], "QUORUM", null)
                        wrapper.executeSyncStatement(s1)
                        wrapper.executeSyncStatement(s2)
                        i++
                        if (i % 1000 == 0) {
                            println "thread ${currentThread().id} at $i"
                        }
                    }
                }
            })
        }
        return list
    }

    List<Thread> shardedWorkerList() {
        List<Thread> list = []
        4.times {
            list.add(new Thread() {
                void run() {
                    println "started worker ${currentThread().id}"
                    DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])
                    int i = 0
                    long lastsec = 0
                    while (true) {
                        Date dt = new Date()
                        long second = dt.time.intdiv(1000)
                        if (second != lastsec) {  // only query db once per second (this could skip seconds under load...)
                            lastsec = second
                            long millisBefore = new Date().time
                            St st = wrapper.prepareSt("ticker", select1(""+calculateShard(dt.time)), [second] as Object[], "QUORUM", null)
                            ResultSet rs = wrapper.executeSyncStatement(st)
                            int size = rs.all().size()
                            long millisAfter = new Date().time
                            println "--->READER: for sec: $second ev: $size resp: ${millisAfter - millisBefore}"
                            i++
                            if (i % 1000 == 0) {
                                println "thread ${currentThread().id} at $i"
                            }
                        }
                    }
                }
            })
        }
        return list
    }

    void createShardedSchema(DriverWrapper wrapper) {
        shardCount.times {
            wrapper.executeDirectUpdate(keyspace, CREATE_scheduleBySecond(it), null, "ALL", null)
            wrapper.executeDirectUpdate(keyspace, CREATE_ScheduleByGroup(it), null, "ALL", null)
            wrapper.executeDirectUpdate(keyspace, alter1(it), null, "ALL", null)
            wrapper.executeDirectUpdate(keyspace, alter2(it), null, "ALL", null)
        }

    }

    void 'reproduce TTL non-sharded'() {
        given:
        // Silence mortals!
        RESTFunctionalTestSpec.setLogLevel('org.springframework', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('org.apache', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('io.netty', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('com.datastax', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('drv.cassdriver', "ERROR")

        DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])

        when:
        List<Thread> workers = workerList()
        List<Thread> schedulers = schedulerList()
        workers*.start()
        schedulers*.start()

        then:
        sleep(5000000) // sleep 1 hour to let the threads do their thing
    }

    void 'TTL sharded'() {
        given:
        // Silence mortals!
        RESTFunctionalTestSpec.setLogLevel('org.springframework', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('org.apache', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('io.netty', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('com.datastax', "ERROR")
        RESTFunctionalTestSpec.setLogLevel('drv.cassdriver', "ERROR")

        DriverWrapper wrapper = new DriverWrapper(autoStart: true, clusterContactNodes: cassIPs[r.nextInt(3)])

        when:
        List<Thread> workers = shardedWorkerList()
        List<Thread> schedulers = shardedSchedulerList()
        workers*.start()
        schedulers*.start()

        then:
        sleep(5000000) // sleep 1 hour to let the threads do their thing
    }

    /**
     *
     * analysis: Some amount of data needs to accumulate before you start cranking the CPU and stressing.
     *
     * ... I think the sstable size and compaction is the problem, with a bit more data we'll probably be
     * able to stress it even more and full reproduce the problem.
     *
     * Rather than 5 minutes, we should accumulate an hour or two's worth of data, and see how compaction
     * starts to impact things.
     *
     * Then we will do the windowing and see how that does.
     *
     * TODO: reader threads with performance checks.
     */

    // ok, let's setup an i3-4x or 8x would be a good docker test bed.

    // - write sharded version of test
    // - setup a simple machine in AWS using ST accounts (will I need jumpbox/bastions?)
    // - setup a i3-8x and setup the code and docker and run test to reproduce compaction problems
    // - then run test with shards to see if compaction
    // - then see if it's even better with truncates

}

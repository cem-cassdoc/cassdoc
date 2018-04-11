package cassutil

import com.datastax.driver.core.ColumnMetadata
import com.datastax.driver.core.DataType
import com.datastax.driver.core.TableMetadata
import cwdrg.util.json.JSONUtil
import drv.cassdriver.DriverWrapper

class TableSchemaSerialize {

    static void main(String[] args) {
        // TODO: ArgMap
        // TODO: ArgMap non-defaulted annotation
        String keyspace
        String table
        String clusterIPs = '127.0.0.1'
        String port = 'DEFAULT'

        DriverWrapper drv = new DriverWrapper(autoStart: true, clusterContactNodes: clusterIPs, clusterPort: port)
        drv.initDataSources(true)
        TableMetadata tableMeta = drv.cluster.metadata.getKeyspace(keyspace).getTable(table)

        println (JSONUtil.serialize(tableMeta))


    }

}


EmbeddedCassandraServerHelper.startEmbeddedCassandra

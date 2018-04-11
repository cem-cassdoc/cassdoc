package cassdoc.commands.retrieve

import groovy.transform.CompileStatic

import java.nio.ByteBuffer

import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.IDUtil
import cassdoc.OperationContext

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Token
import com.datastax.driver.core.utils.Bytes

import cwdrg.util.json.JSONUtil

//-----------------
//---- QUERIES ----
//-----------------

// --- rowprocessors (streaming/paged

// 1) code the initiateQuery, which constructs the CQL query and initiates the execution
// 2) call nextRow until it returns null
// 3) after each nextRow, if desired, check if there was a new Partition and get the afterproducts of the previous completed partition

@CompileStatic
abstract class RowProcessor {
    boolean newPartition
    long pageCount = 0
    long rowCount = 0
    long partitionRowCount = 0
    int fetchNextPageThreshold = 5000

    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {}

    Object[] nextRow() {}
}

@CompileStatic
abstract class CassandraPagedRowProcessor extends RowProcessor {
    ResultSet rs = null
    private Token lastToken = null

    /**
     * This method is called for every row encountered
     *
     * @param row
     * @return
     */
    Object[] processRow(Row row) {}

    /**
     * This optional method initializes any data structures tracking data across column/clustering keys in a partition.
     *
     */
    void initNewPartition() {}

    /**
     * This optional method is called whenever a new partition key is encountered,  in case
     * there are some final products/packaging/processing needed before the next partition tracking is done
     *
     * Examples: summing columns in a row, tracking column keys in a row, etc
     */
    void completeFinishedPartition() {}

    /**
     * An optional method, the code using the RowProcessor, if indicated by the newPartition stateful property, can call this method to get the
     * final products/data structures/information that has been accumulated and finalized by completeFinishedPartition()
     *
     * @return Object[]
     */
    Object[] getFinishedPartitionData() { null }

    Object[] nextRow() {
        Row row = rs.one()
        if (row == null) {
            newPartition = false
            return null
        } else {
            rowCount++
            partitionRowCount++
            Token currentToken = row.partitionKeyToken
            if (currentToken == lastToken) {
                newPartition = false
                if (lastToken == null) {
                    pageCount++ // increment to 1 as soon as we get a legitimate row
                    lastToken = currentToken
                } else {
                    newPartition = true
                    completeFinishedPartition()
                    lastToken = currentToken
                    initNewPartition()
                }
            }

            if (rs.getAvailableWithoutFetching() == fetchNextPageThreshold && !rs.isFullyFetched()) {
                pageCount++
                rs.fetchMoreResults()
            }
        }

        return processRow(row)
    }

}


@CompileStatic
class GetDocAttrsRP extends CassandraPagedRowProcessor {
    String docUUID = null
    boolean writetimeCol = false
    boolean tokenCol = false
    boolean attrMetaCol = false

    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = QueryCmd.resolveConsistency(detail, opctx)

        String space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        StringBuilder cql = new StringBuilder(64)
        cql << "SELECT token(e),e,p,d,t,zv"
        if (detail.attrWritetimeMeta != null) {
            cql << ",writetime(" << detail.attrWritetimeMeta << ")"
            writetimeCol = true
        }
        if (detail.attrMetaDataMeta || detail.attrMetaIDMeta) {
            cql << ",z_md"
            attrMetaCol = true
        }
        cql << " FROM " << space << ".p_" << suffix << " WHERE e = ?"
        Object[] cqlargs = [docUUID] as Object[]
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        // initiate
        rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
    }

    Object[] processRow(Row row) {
        Object[] attr = [
                row.getString("p"),
                row.getString("t"),
                row.getString("d"),
                row.getUUID("zv"),
                writetimeCol ? row.getLong(6) : null,
                tokenCol ? row.partitionKeyToken.value.toString() : null,
                attrMetaCol ? row.getString("z_md") : null
        ] as Object[]
        return attr
    }
}


@CompileStatic
class QueryToListOfStrArr extends CassandraPagedRowProcessor {
    String query
    private int rowWidth = -1

    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = QueryCmd.resolveConsistency(detail, opctx)

        String space = opctx.space
        rs = svcs.driver.initiateQuery(space, query, args, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
        rowWidth = rs.getColumnDefinitions().size()
    }

    Object[] processRow(Row row) {
        Object[] data = new Object[rowWidth]
        for (int i = 0; i < rowWidth; i++) {
            DataType dt = row.columnDefinitions.getType(i)
            switch (dt) {
                case DataType.ascii():
                case DataType.text():
                case DataType.varchar():
                    data[i] = row.getString(i)
                    break
                case DataType.timestamp():
                    data[i] = row.getTimestamp(i)?.time as String
                    break
                case DataType.cboolean():
                    data[i] = row.getBool(i) as String
                    break
                case DataType.cint():
                    data[i] = row.getInt(i) as String
                    break
                case DataType.bigint():
                case DataType.counter():
                    data[i] = row.getLong(i) as String
                    break
                case DataType.varint():
                    data[i] = row.getVarint(i) as String
                    break
                case DataType.cfloat():
                    data[i] = row.getFloat(i) as String
                    break
                case DataType.cdouble():
                    data[i] = row.getDouble(i) as String
                    break
                case DataType.decimal():
                    data[i] = row.getDecimal(i) as String
                    break
                case DataType.timeuuid():
                case DataType.uuid():
                    data[i] = row.getUUID(i).toString()
                    break
                case DataType.blob():
                    ByteBuffer buffer = row.getBytes(i)
                    byte[] wrapped = Bytes.getArray(buffer)
                    data[i] = Base64.encoder.encodeToString(wrapped)
                    break
                default:
                    Object colval = row.getObject(i)
                    data[i] = colval == null ? null : JSONUtil.serialize(colval)
            }

        }
        return data
    }
}


@CompileStatic
class QueryToListOfObjArr extends CassandraPagedRowProcessor {
    String query
    int rowSize = -1

    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = QueryCmd.resolveConsistency(detail, opctx)

        String space = opctx.space
        rs = svcs.driver.initiateQuery(space, query, args, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
        rowSize = rs.getColumnDefinitions().size()
    }

    Object[] processRow(Row row) {
        Object[] data = new Object[rowSize]
        for (int i = 0; i < rowSize; i++) {
            DataType dt = row.columnDefinitions.getType(i)
            switch (dt) {

                case DataType.ascii():
                case DataType.text():
                case DataType.varchar():
                    data[i] = row.getString(i)
                    break

                case DataType.timestamp():
                    data[i] = row.getTimestamp(i)
                    break

                case DataType.cboolean():
                    data[i] = row.getBool(i)
                    break

                case DataType.cint():
                    data[i] = row.getInt(i)
                    break

                case DataType.bigint():
                case DataType.counter():
                    data[i] = row.getLong(i)
                    break

                case DataType.varint():
                    data[i] = row.getVarint(i)
                    break

                case DataType.cfloat():
                    data[i] = row.getFloat(i)
                    break

                case DataType.cdouble():
                    data[i] = row.getDouble(i)
                    break

                case DataType.decimal():
                    data[i] = row.getDecimal(i)
                    break

                case DataType.timeuuid():
                case DataType.uuid():
                    data[i] = row.getUUID(i)
                    break

                case DataType.blob():
                    ByteBuffer buffer = row.getBytes(i)
                    byte[] wrapped = Bytes.getArray(buffer)
                    data[i] = wrapped
                    break

                default:
                    Object colval = row.getObject(i)
                    data[i] = colval
            }

        }
        return data
    }
}

@CompileStatic
class IndexTableRP extends CassandraPagedRowProcessor {
    String i1 = ""
    String i2 = ""
    String i3 = ""
    String k1 = ""
    String k2 = ""
    String k3 = ""

    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = QueryCmd.resolveConsistency(detail, opctx)
        String space = opctx.space
        StringBuilder cql = new StringBuilder(32)
        cql << "SELECT token(i1,i2,i3,k1,k2,k3),v1,v2,v3,d FROM ${space}.i WHERE i1 = ? AND i2 = ? AND i3 = ? AND k1 = ? AND k2 = ? AND k3 = ?"
        rs = svcs.driver.initiateQuery(space, cql.toString(), [i1, i2, i3, k1, k2, k3] as Object[], consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
    }

    Object[] processRow(Row row) {
        Object[] data = [
                row.getString(1),
                row.getString(2),
                row.getString(3)] as Object[]
        return data
    }
}

@CompileStatic
class EntityTableSecondaryIndexRP extends CassandraPagedRowProcessor {
    String table
    String column
    String columnType
    Object columnValue

    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = QueryCmd.resolveConsistency(detail, opctx)
        String space = opctx.space
        StringBuilder cql = new StringBuilder(32)
        cql << "SELECT token(e),e FROM ${space}.${table} WHERE ${column} = ?"
        rs = svcs.driver.initiateQuery(space, cql.toString(), [columnValue] as Object[], consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
    }

    Object[] processRow(Row row) {
        Object[] data = [row.getString(1)] as Object[]
        return data
    }
}
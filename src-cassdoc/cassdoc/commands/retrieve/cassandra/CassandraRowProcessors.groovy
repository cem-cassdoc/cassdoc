package cassdoc.commands.retrieve.cassandra

import groovy.transform.CompileStatic

import java.nio.ByteBuffer

import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.IDUtil
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelKey

import com.datastax.driver.core.DataType
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
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
class GetDocAttrsRP extends CassandraPagedRowProcessor
{
  String docUUID = null
  boolean writetimeCol = false
  boolean tokenCol = false
  boolean attrMetaCol = false

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)

    String space = opctx.space
    String suffix = IDUtil.idSuffix(docUUID)
    StringBuilder cql = new StringBuilder(64)
    cql << "SELECT token(e),e,p,d,t,zv"
    if (detail.attrWritetimeMeta != null) {
      cql << ",writetime(" << detail.attrWritetimeMeta << ")";
      writetimeCol = true
    }
    if (detail.attrMetaDataMeta || detail.attrMetaIDMeta) {
      cql << ",z_md";
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
class QueryToListOfStrArr extends CassandraPagedRowProcessor
{
  String query
  private int rowSize = -1

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)

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
          data[i] = row.getString(i);
          break;

        case DataType.timestamp():
          data[i] = row.getTimestamp(i)?.time?.toString()
          break;

        case DataType.cboolean():
          data[i] = row.getBool(i)?.toString();
          break;

        case DataType.cint():
          data[i] = row.getInt(i)?.toString();
          break;

        case DataType.bigint():
        case DataType.counter():
          data[i] = row.getLong(i)?.toString();
          break;

        case DataType.varint():
          data[i] = row.getVarint(i)?.toString();
          break;

        case DataType.cfloat():
          data[i] = row.getFloat(i)?.toString();
          break;

        case DataType.cdouble():
          data[i] = row.getDouble(i)?.toString();
          break;

        case DataType.decimal():
          data[i] = row.getDecimal(i)?.toString();
          break;

        case DataType.timeuuid():
        case DataType.uuid():
          data[i] = row.getUUID(i);
          break;

        case DataType.blob():
          ByteBuffer buffer = row.getBytes(i);
          byte[] wrapped = Bytes.getArray(buffer);
          data[i] = Base64.encoder.encodeToString(wrapped)
          break;

        default:
          Object colval = row.getObject(i)
          data[i] = colval == null ? null : JSONUtil.serialize(colval)
      }

    }
    return data
  }
}



@CompileStatic
class QueryToListOfObjArr extends CassandraPagedRowProcessor
{
  String query
  int rowSize = -1

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)

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
          data[i] = row.getString(i);
          break;

        case DataType.timestamp():
          data[i] = row.getTimestamp(i);
          break;

        case DataType.cboolean():
          data[i] = row.getBool(i);
          break;

        case DataType.cint():
          data[i] = row.getInt(i);
          break;

        case DataType.bigint():
        case DataType.counter():
          data[i] = row.getLong(i);
          break;

        case DataType.varint():
          data[i] = row.getVarint(i);
          break;

        case DataType.cfloat():
          data[i] = row.getFloat(i);
          break;

        case DataType.cdouble():
          data[i] = row.getDouble(i);
          break;

        case DataType.decimal():
          data[i] = row.getDecimal(i);
          break;

        case DataType.timeuuid():
        case DataType.uuid():
          data[i] = row.getUUID(i);
          break;

        case DataType.blob():
          ByteBuffer buffer = row.getBytes(i);
          byte[] wrapped = Bytes.getArray(buffer);
          data[i] = wrapped
          break;

        default:
          Object colval = row.getObject(i)
          data[i] = colval
      }

    }
    return data
  }
}

@CompileStatic
class IndexTableRP extends CassandraPagedRowProcessor
{
  String i1 = ""
  String i2 = ""
  String i3 = ""
  String k1 = ""
  String k2 = ""
  String k3 = ""

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
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
class EntityTableSecondaryIndexRP extends CassandraPagedRowProcessor
{
  String table
  String column
  String columnType
  Object columnValue

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
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

@CompileStatic
class DocAttrListRP extends CassandraPagedRowProcessor
{
  String docUUID

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
    String space = opctx.space
    String suffix = IDUtil.idSuffix(docUUID)
    StringBuilder cql = new StringBuilder(32)
    cql << "SELECT token(e),p FROM ${space}.p_${suffix}"
    rs = svcs.driver.initiateQuery(space, cql.toString(), null, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }

  Object[] processRow(Row row) {
    Object[] data = [row.getString(1)] as Object[]
    return data
  }
}

@CompileStatic
class GetDocRP extends CassandraPagedRowProcessor
{
  String docUUID
  private boolean writetimeCol
  private boolean tokenCol
  private boolean metaCol

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
    String space = opctx.space
    String suffix = IDUtil.idSuffix(docUUID)
    StringBuilder cql = new StringBuilder(64)
    cql << "SELECT token(e),e,a0,zv"
    if (detail.docWritetimeMeta != null) {
      cql << ",writetime(" << detail.docWritetimeMeta << ")"
      writetimeCol = true
    }
    if (detail.docMetaIDMeta) {
      metaCol = true
      cql << ",z_md"
    }
    if (detail.docTokenMeta) {
      tokenCol = true;
    }
    cql << " FROM " << space << ".e_" << suffix << " WHERE e = ?"
    Object[] cqlargs = [docUUID] as Object[]

    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])

    rs = svcs.driver.initiateQuery(space, cql.toString(), [docUUID] as Object[], consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }

  Object[] processRow(Row row) {
    Object[] data = [
      row.getString("a0"),
      row.getUUID("zv"),
      writetimeCol? row.getLong(4):null,
      tokenCol?row.partitionKeyToken.value.toString():null,
      metaCol?row.getString("z_md"):null] as Object[]
    return data
  }

}


@CompileStatic
class GetAttrRP extends CassandraPagedRowProcessor
{
  String docUUID
  String attrName
  private boolean writetimeCol
  private boolean tokenCol
  private boolean attrMetaCol

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
    String space = opctx.space
    String suffix = IDUtil.idSuffix(docUUID)
    StringBuilder cql = new StringBuilder(64)
    cql << "SELECT token(e),e,p,d,t,zv"
    if (detail.attrWritetimeMeta != null) { writetimeCol = true; cql << ",writetime(" << detail.attrWritetimeMeta << ")" }
    if (detail.attrTokenMeta) { tokenCol = true; cql << ",token(e)" }
    if (detail.attrMetaIDMeta || detail.attrMetaDataMeta) { attrMetaCol = true; cql << ",z_md" }
    cql << " FROM " << space << ".p_" << suffix << " WHERE e = ? and p = ?"
    Object[] cqlargs = [docUUID, attrName] as Object[]
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])

    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }

  Object[] processRow(Row row) {
    Object[] data = [
      row.getString("t"),
      row.getString("d"),
      row.getUUID("zv"),
      writetimeCol ? row.getLong(6) : null,
      tokenCol ? row.partitionKeyToken.value.toString() : null,
      attrMetaCol ? row.getString("z_md") : null
    ] as Object[]
    return data
  }

}

@CompileStatic
class GetAttrMetaRP extends CassandraPagedRowProcessor
{
  String docUUID
  String attrName
  private boolean writetimeCol
  private boolean tokenCol
  private boolean attrMetaCol

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
    String space = opctx.space
    String suffix = IDUtil.idSuffix(docUUID)
    StringBuilder cql = new StringBuilder(64)
    cql << "SELECT token(e),e,p,zv"
    if (detail.attrWritetimeMeta != null) { writetimeCol = true; cql << ",writetime(" << detail.attrWritetimeMeta << ")" }
    if (detail.attrTokenMeta) { tokenCol = true; cql << ",token(e)" }
    if (detail.attrMetaIDMeta || detail.attrMetaDataMeta) { attrMetaCol = true; cql << ",z_md" }
    cql << " FROM " << space << ".p_" << suffix << " WHERE e = ? and p = ?"
    Object[] cqlargs = [docUUID, attrName] as Object[]
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])

    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }

  Object[] processRow(Row row) {
    Object[] data = [
      row.getUUID("zv"),
      writetimeCol ? row.getLong(4) : null,
      tokenCol ? row.partitionKeyToken.value.toString() : null,
      attrMetaCol ? row.getString("z_md") : null
    ] as Object[]
    return data
  }

}

class BaseGetRelsRP extends CassandraPagedRowProcessor
{
  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {

  }

  Object[] processRow(Row row) {
    Rel rel = new Rel(
        p1:row.getString("p1"),
        p2:row.getString("p2"),
        p3:row.getString("p3"),
        p4:row.getString("p4"),
        ty1:row.getString("ty1"),
        c1:row.getString("c1"),
        c2:row.getString("c2"),
        c3:row.getString("c3"),
        c4:row.getString("c4"),
        ty2:row.getString("ty2"),
        d:row.getString("d"),
        lk:row.getString("link"),
        z_md:row.getString("z_md")
        )
    return [rel] as Object[]
  }

}

class GetAttrRelsRP extends BaseGetRelsRP {
  String p1 = ""
  List<String> ty1s = []
  String p2 = ""

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)

    String space = opctx.space
    String cql = "SELECT token(p1),p1,ty1,p2,p3,p4,c1,c2,c3,c4,ty2,d,link,z_md FROM ${space}.r WHERE p1 = ? AND ty1 in ? AND p2 = ?"
    Object[] cqlargs = [p1, ty1s, p2] as Object[]
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])
    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }
}

class GetDocRelsRP extends BaseGetRelsRP
{
  String p1 = ""
  String ty1 = ""
  String p2 = ""
  String p3 = ""
  String p4 = ""
  String c1 = ""
  String c2 = ""
  String c3 = ""
  String c4 = ""
  String ty2 = ""

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
    String space = opctx.space
    String cql = "SELECT token(p1),p1,ty1,p2,p3,p4,c1,c2,c3,c4,ty2,d,link,z_md FROM ${space}.r WHERE p1 = ?"
    Object[] cqlargs = [p1] as Object[]
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])
    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }
}

class GetDocRelsForTypeRP extends GetDocRelsRP
{
  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)
    String space = opctx.space
    String cql = "SELECT token(p1),p1,ty1,p3,p4,c1,c2,c3,c4,ty2,d,link,z_md FROM ${space}.r WHERE p1 = ? AND ty1 = ?"
    Object[] cqlargs = [p1, ty1] as Object[]
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])
    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }
}

class GetAttrRelsForTypeRP extends GetDocRelsRP
{
  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)

    String space = opctx.space
    String cql = "SELECT token(p1),p1,ty1,p2,p3,p4,c1,c2,c3,c4,ty2,d,link,z_md FROM ${space}.r WHERE p1 = ? AND ty1 = ? AND p2 = ?"
    Object[] cqlargs = [p1, ty1, p2] as Object[]
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])
    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }
}

class GetRelKeyRP extends BaseGetRelsRP
{
  RelKey relKey;

  void analyzeRelKey(StringBuilder where, List arglist)
  {
    // p1 is required, since it is the hash key / row key
    where << "p1 = ? "
    arglist.add(relKey.p1)

    if (!relKey.isty1) {
      // getting all relations for the provided uuid (aka p1)
      return
    } else {
      // type specified
      where << "AND ty1 = ? "
      arglist.add(relKey.ty1)
      if (relKey.isp2) {
        where << " AND p2 = ? "
        arglist.add(relKey.p2)
      } else {
        // getting all relations beginning with type ty1 for p1
        return
      }
    }

    // p1, ty fields, and p2 have been determined, we divine if a more detailed p3/p4 are desired, and if there are child components
    if (!relKey.isp3) {
      if (!relKey.isc1) {
        return
      } else {
        where << "AND p3 = '' AND p4 = '' AND c1 = ?"
        arglist.add(relKey.c1)
      }
    } else {
      where << "AND p3 = ? "
      arglist.add(relKey.p3)
      if (!relKey.isp4) {
        if (!relKey.isc1) {
          return
        } else {
          where << "AND p4 = '' AND c1 = ? "
          arglist.add(relKey.c1)
        }
      } else {
        where << "AND p4 = ? "
        arglist.add(relKey.p4)
        if (!relKey.isc1) {
          return
        } else {
          where << "AND c1 = ? "
          arglist.add(relKey.c1)
        }
      }
    }

    if (!relKey.isty2) {
      // see if there are more c fields
      if (relKey.isc2) {
        where << "AND c2 = ? "
        arglist.add(relKey.c2)
      }

      if (relKey.isc3) {
        where << "AND c3 = ? "
        arglist.add(relKey.c3)
      }

      if (relKey.isc4) {
        where << "AND c4 = ? "
        arglist.add(relKey.c4)
      }
    } else {
      if (relKey.isc2) {
        if (relKey.isc3) {
          if (relKey.isc4) {
            where << "AND c2 = ? AND c3 = ? and c4 = ? "
            arglist.add(relKey.c2)
            arglist.add(relKey.c3)
            arglist.add(relKey.c4)
          } else {
            where << "AND c2 = ? AND c3 = ? and c4 = '' "
            arglist.add(relKey.c2)
            arglist.add(relKey.c3)
          }
        } else {
          where << "AND c2 = ? AND c3 = '' and c4 = '' "
          arglist.add(relKey.c2)
        }
      }

      if (relKey.isty2) {
        where << "AND ty2 = ? "
        arglist.add(relKey.ty2)
      }
    }

  }

  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
  {
    String consistency = resolveConsistency(detail,opctx)

    String space = opctx.space
    StringBuilder where = new StringBuilder(64)
    List arglist = []

    analyzeRelKey(where, arglist)

    String cql = "SELECT token(p1),p1,ty1,p2,p3,p4,c1,c2,c3,c4,ty2,d,link,z_md FROM ${space}.r WHERE " + where
    Object[] cqlargs = arglist.toArray()
    if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
      cql,
      cqlargs,
      consistency,
      null] as Object[])
    rs = svcs.driver.initiateQuery(space, cql.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
  }


}



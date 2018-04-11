package cassdoc.commands.retrieve

import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringUtils

import cassdoc.Cmd
import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.IDUtil
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelKey

import com.datastax.driver.core.Row

import cwdrg.lg.annotation.Log
import drv.cassdriver.CQLException
import drv.cassdriver.RowCallbackHandler


//-----------------
//---- QUERIES ----
//-----------------

// --- commands (not streaming/paged)


@CompileStatic
abstract class QueryCmd extends Cmd {
    static String resolveConsistency(Detail detail, OperationContext opctx) {
        if (detail != null && StringUtils.isNotEmpty(detail.readConsistency))
            return detail.readConsistency
        if (StringUtils.isNotEmpty(opctx.readConsistency))
            return opctx.readConsistency
        return "ONE"
    }

}


@CompileStatic
class GetDocAttrs extends QueryCmd {
    String docUUID

    GetDocAttrsRCH queryCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)
        GetDocAttrsRCH rch = new GetDocAttrsRCH()
        String space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        StringBuilder cql = new StringBuilder(64)
        cql << "SELECT e,p,d,t,zv"
        if (detail.attrWritetimeMeta != null) {
            cql << ",writetime(" << detail.attrWritetimeMeta << ")"
            rch.writetimeCol = true
        }
        if (detail.attrTokenMeta) {
            rch.tokenCol = true
            cql << ",token(e)"
        }
        if (detail.attrMetaIDMeta || detail.attrMetaDataMeta) {
            rch.attrMetaCol = true
            cql << ",z_md"
        }
        cql << " FROM " << space << ".p_" << suffix << " WHERE e = ?"
        Object[] cqlargs = [docUUID] as Object[]
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql.toString(), cqlargs, rch, consistency)
        return rch
    }

}

@CompileStatic
class GetDocAttrsRCH implements RowCallbackHandler {
    boolean writetimeCol = false
    boolean tokenCol = false
    boolean attrMetaCol = false
    List<Object[]> attrs = []

    void processRow(Row row) throws CQLException {
        Object[] attr = [
                row.getString("p"),
                row.getString("t"),
                row.getString("d"),
                row.getUUID("zv"),
                writetimeCol ? row.getLong(5) : null,
                tokenCol ? row.partitionKeyToken.value.toString() : null,
                attrMetaCol ? row.getString("z_md") : null] as Object[]
        attrs.add(attr)
    }
}

@Log
@CompileStatic
class GetAttrCmd extends QueryCmd {
    String docUUID
    String attrName

    GetAttrRCH queryCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)
        GetAttrRCH rch = new GetAttrRCH()
        String space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        StringBuilder cql = new StringBuilder(64)
        cql << "SELECT e,p,d,t,zv"
        if (detail.attrWritetimeMeta != null) {
            rch.writetimeCol = true
            cql << ",writetime(" << detail.attrWritetimeMeta << ")"
        }
        if (detail.attrTokenMeta) {
            rch.tokenCol = true
            cql << ",token(e)"
        }
        if (detail.attrMetaIDMeta || detail.attrMetaDataMeta) {
            rch.attrMetaCol = true
            cql << ",z_md"
        }
        cql << " FROM " << space << ".p_" << suffix << " WHERE e = ? and p = ?"
        Object[] cqlargs = [docUUID, attrName] as Object[]
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql.toString(), cqlargs, rch, consistency)
        return rch
    }
}

@CompileStatic
class GetAttrRCH implements RowCallbackHandler {
    boolean writetimeCol = false
    boolean tokenCol = false
    boolean attrMetaCol = false
    String valType
    String data
    UUID version
    Long writetime
    String token
    String attrMetaID

    void processRow(Row row) throws CQLException {
        valType = row.getString("t")
        data = row.getString("d")
        version = row.getUUID("zv")
        writetime = writetimeCol ? row.getLong(6) : null
        token = tokenCol ? row.partitionKeyToken.value.toString() : null
        attrMetaID = attrMetaCol ? row.getString("z_md") : null
    }
}

@Log
@CompileStatic
class GetAttrMetaCmd extends QueryCmd {
    String docUUID
    String attrName

    GetAttrMetaRCH queryCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)

        GetAttrMetaRCH rch = new GetAttrMetaRCH()
        String space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        StringBuilder cql = new StringBuilder(64)
        cql << "SELECT e,p,zv"
        if (detail.attrWritetimeMeta != null) {
            rch.writetimeCol = true
            cql << ",writetime(" << detail.attrWritetimeMeta << ")"
        }
        if (detail.attrTokenMeta) {
            rch.tokenCol = true
            cql << ",token(e)"
        }
        if (detail.attrMetaIDMeta || detail.attrMetaDataMeta) {
            rch.attrMetaCol = true
            cql << ",z_md"
        }
        cql << " FROM " << space << ".p_" << suffix << " WHERE e = ? and p = ?"
        Object[] cqlargs = [docUUID, attrName] as Object[]
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql.toString(), cqlargs, rch, consistency)
        return rch
    }
}

@CompileStatic
class GetAttrMetaRCH implements RowCallbackHandler {
    boolean writetimeCol = false
    boolean tokenCol = false
    boolean attrMetaCol = false
    UUID version
    Long writetime
    String token
    String attrMetaID

    void processRow(Row row) throws CQLException {
        version = row.getUUID("zv")
        writetime = writetimeCol ? row.getLong(3) : null
        token = tokenCol ? row.partitionKeyToken.value.toString() : null
        attrMetaID = attrMetaCol ? row.getString("z_md") : null
    }
}


@CompileStatic
class GetRelsCmd extends QueryCmd {
    String p1 = ""
    String ty1 = ""
    String ty2 = ""
    String ty3 = ""
    String ty4 = ""
    String p2 = ""
    String p3 = ""
    String p4 = ""
    String c1 = ""
    String c2 = ""
    String c3 = ""
    String c4 = ""

    GetRelsRCH queryCassandraDocRels(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)
        String space = opctx.space
        String cql = "SELECT p1,ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4,d,link,z_md FROM ${space}.r WHERE p1 = ?"
        Object[] cqlargs = [p1] as Object[]
        GetRelsRCH rch = new GetRelsRCH()
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql, cqlargs, rch, consistency)
        return rch
    }

    GetRelsRCH queryCassandraDocRelsForType(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)
        String space = opctx.space
        String cql = "SELECT p1,ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4,d,link,z_md FROM ${space}.r WHERE p1 = ? AND ty1 = ?"
        Object[] cqlargs = [p1, ty1] as Object[]
        GetRelsRCH rch = new GetRelsRCH()
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql, cqlargs, rch, consistency)
        return rch
    }

    GetRelsRCH queryCassandraAttrRelsForType(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)

        String space = opctx.space
        String cql = "SELECT p1,ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4,d,link,z_md FROM ${space}.r WHERE p1 = ? AND ty1 = ? AND ty2='' AND ty3='' AND ty4='' AND p2 = ?"
        Object[] cqlargs = [p1, ty1, p2] as Object[]
        GetRelsRCH rch = new GetRelsRCH()
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql, cqlargs, rch, consistency)
        return rch

    }

}

class GetRelKeyCmd extends QueryCmd {
    RelKey relKey

    /**
     * we're trying to guess how specific a query to submit based on if a relation type, subparts
     * of the parent, and if children were set in the relKey. We use marker booleans to track if the
     * setter was invoked for the key components, as a way of divining intent
     *
     *   TODO: more complicated where clauses (range slices, INs)
     *
     * @param where
     * @param arglist
     */
    void analyzeRelKey(StringBuilder where, List arglist) {
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
            if (!relKey.isty2) {
                if (relKey.isp2) {
                    where << "AND ty2 = '' AND ty3 = '' AND ty4 = '' AND p2 = ? "
                    arglist.add(relKey.p2)
                } else {
                    // getting all relations beginning with type ty1 for p1
                    return
                }
            } else {
                where << "AND ty2 = ? "
                arglist.add(relKey.ty2)
                if (!relKey.isty3) {
                    if (relKey.isp2) {
                        where << "AND ty3 = '' AND ty4 = '' AND p2 = ? "
                        arglist.add(relKey.p2)
                    } else {
                        // getting relations matching ty1 and ty2 for p1
                        return
                    }
                } else {
                    where << "AND ty3 = ? "
                    arglist.add(relKey.ty3)
                    if (!relKey.isty4) {
                        if (relKey.isp2) {
                            where << "AND ty4 = '' AND p2 = ? "
                            arglist.add(relKey.p2)
                        } else {
                            // getting relations matching ty1, ty2, ty3 for p1
                            return
                        }
                    } else {
                        where << "AND ty4 = ? "
                        arglist.add(relKey.ty4)
                        if (!relKey.isp2) {
                            // getting relations matching ty1, ty2, ty3, ty4  for p1
                            return
                        } else {
                            where << "AND p2 = ? "
                            arglist.add(relKey.p2)
                        }
                    }
                }
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

    }

    GetRelsRCH queryCassandraRelKey(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)

        String space = opctx.space
        StringBuilder where = new StringBuilder(64)
        List arglist = []

        analyzeRelKey(where, arglist)

        String cql = "SELECT p1,ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4,d,link,z_md FROM ${space}.r WHERE " + where
        Object[] cqlargs = arglist.toArray()
        GetRelsRCH rch = new GetRelsRCH()
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql, cqlargs, rch, consistency)
        return rch
    }

}


@CompileStatic
class GetAttrRelsCmd extends QueryCmd {
    String p1 = ""
    Set<String> ty1s = [] as HashSet
    String p2 = ""

    GetRelsRCH queryCassandraAttrRels(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)

        String space = opctx.space
        String cql = "SELECT p1,ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4,d,link,z_md FROM ${space}.r WHERE p1 = ? AND ty1 in ? AND ty2='' AND ty3='' AND ty4='' AND p2 = ?"
        Object[] cqlargs = [p1, ty1s, p2] as Object[]
        GetRelsRCH rch = new GetRelsRCH()
        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql, cqlargs, rch, consistency)
        return rch

    }

}


@CompileStatic
class GetRelsRCH implements RowCallbackHandler {
    List<Rel> rels = []

    void processRow(Row row) throws CQLException {
        Rel rel = new Rel(
                p1: row.getString("p1"),
                p2: row.getString("p2"),
                p3: row.getString("p3"),
                p4: row.getString("p4"),
                ty1: row.getString("ty1"),
                ty2: row.getString("ty2"),
                ty3: row.getString("ty3"),
                ty4: row.getString("ty4"),
                c1: row.getString("c1"),
                c2: row.getString("c2"),
                c3: row.getString("c3"),
                c4: row.getString("c4"),
                d: row.getString("d"),
                lk: row.getString("link"),
                z_md: row.getString("z_md")
        )
        rels.add(rel)
    }
}


@CompileStatic
class GetDoc extends QueryCmd {
    String docUUID

    GetDocRCH queryCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        String consistency = resolveConsistency(detail, opctx)

        String space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        GetDocRCH rch = new GetDocRCH()
        StringBuilder cql = new StringBuilder(64)
        cql << "SELECT e,a0,zv"
        if (detail.docWritetimeMeta != null) {
            cql << ",writetime(" << detail.docWritetimeMeta << ")"
            rch.writetimeCol = true
        }
        if (detail.docMetaIDMeta) {
            rch.metaCol = true
            cql << ",z_md"
        }
        if (detail.docTokenMeta) {
            rch.tokenCol = true
            cql << ",token(e)"
        }
        cql << " FROM " << space << ".e_" << suffix << " WHERE e = ?"
        Object[] cqlargs = [docUUID] as Object[]

        if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                cql,
                cqlargs,
                consistency,
                null] as Object[])
        svcs.driver.executeQueryCQL(space, cql.toString(), cqlargs, rch, consistency)
        return rch
    }

}

@CompileStatic
class GetDocRCH implements RowCallbackHandler {
    boolean writetimeCol = false
    boolean tokenCol = false
    boolean metaCol = false
    String a0
    UUID paxosVer
    String metadata_id
    String token
    Long writetime

    void processRow(Row row) throws CQLException {
        a0 = row.getString("a0")
        paxosVer = row.getUUID("zv")
        if (writetimeCol) {
            writetime = row.getLong(3)
        }
        if (tokenCol) {
            token = row.partitionKeyToken.value.toString()
        }
        if (metaCol) {
            metadata_id = row.getString("z_md")
        }
    }
}



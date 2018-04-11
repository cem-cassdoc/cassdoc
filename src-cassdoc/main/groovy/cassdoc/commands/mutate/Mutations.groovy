package cassdoc.commands.mutate

import com.datastax.driver.core.LocalDate
import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringUtils

import cassdoc.Cmd
import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.FieldValue
import cassdoc.IDUtil
import cassdoc.ListMap
import cassdoc.OperationContext
import cassdoc.RelKey
import cassdoc.TypeConfigurationService
import cwdrg.lg.annotation.Log
import cwdrg.util.json.JSONUtil
import drv.cassdriver.St

@Log
@CompileStatic
abstract class MutationCmd extends Cmd {
    boolean getClearCmd() {
        false
    }
    String[] paxosId = null
    boolean added = false
    String space
    String cql
    Object[] cqlargs

    abstract Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)

    abstract void batch(OperationContext opctx)

    /**
     * optimize allows the Command to analyze the current state of other commands to determine if it doesn't need to be executed
     * ...
     * basic example: delete attribute isn't needed if there is a delete document/rowkey as well
     *
     * @param svcs
     * @param opctx
     * @param detail
     * @return
     */
    abstract boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail)
    //abstract Object execMutationRdbms(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)

    Object execOrPrep(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.executionMode == "immediate") {
            if (opctx.cqlTraceEnabled) opctx.cqlTrace.add([
                    cql,
                    cqlargs,
                    detail.writeConsistency,
                    clearCmd ? opctx.operationClearTimestamp : opctx.operationTimestamp] as Object[])
            // TODO: async
            // TODO: handle PAXOS conditional update
            String consistency = StringUtils.isNotEmpty(detail.writeConsistency) ? detail.writeConsistency : StringUtils.isNotEmpty(opctx.writeConsistency) ? opctx.writeConsistency : "ONE"
            svcs.driver.executeDirectUpdate(space, cql, cqlargs, consistency, clearCmd ? opctx.operationClearTimestamp : opctx.operationTimestamp)
            return null
        } else {
            try {
                St st = new St(keyspace: space, cql: cql, cqlargs: cqlargs, stmt: svcs.driver.prepare(space, cql, cqlargs, detail.writeConsistency, clearCmd ? opctx.operationClearTimestamp : opctx.operationTimestamp))
                return st
            } catch (Exception e) {
                throw log.err("", new RuntimeException("ERROR in statement prepareCtx $cql " + JSONUtil.serialize(cqlargs)))
            }
        }
    }

}

// -------------------
// ---- MUTATIONS ----
// -------------------


@Log
@CompileStatic
class NewDoc extends MutationCmd {
    String docUUID
    String parentUUID
    String parentAttr
    // tags
    // meta

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "E" + docUUID, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        // when is this ever cancelled?
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "INSERT INTO ${space}.e_${suffix} (e,zv,a0) VALUES (?,?,?)"
        cqlargs = [
                docUUID,
                opctx.updateUUID,
                parentUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class ClrIdxVal extends MutationCmd {
    boolean getClearCmd() { true }


    String i1 = ""
    String i2 = ""
    String i3 = ""
    String k1 = ""
    String k2 = ""
    String k3 = ""
    String v1 = ""
    String v2 = ""
    String v3 = ""

    void batch(OperationContext opctx) {
        opctx.batchLeftovers.add(this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }


    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "DELETE FROM ${space}.i WHERE i1 = ? and i2 = ? and i3 = ? and k1 = ? and k2 = ? and k3 = ? and v1 = ? and v2 = ? and v3 = ?"
        cqlargs = [
                i1,
                i2,
                i3,
                k1,
                k2,
                k3,
                v1,
                v2,
                v3] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class InsIdxValOnly extends MutationCmd {
    String i1 = ""
    String i2 = ""
    String i3 = ""
    String k1 = ""
    String k2 = ""
    String k3 = ""
    String v1 = ""
    String v2 = ""
    String v3 = ""

    void batch(OperationContext opctx) {
        opctx.batchLeftovers.add(this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "INSERT INTO ${space}.i (i1,i2,i3,k1,k2,k3,v1,v2,v3) VALUES (?,?,?,?,?,?,?,?,?)"
        cqlargs = [
                i1,
                i2,
                i3,
                k1,
                k2,
                k3,
                v1,
                v2,
                v3] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class UpdFixedCol extends MutationCmd {
    String docUUID
    String colName
    Object value

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "E" + docUUID, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("UpdFixedCol filtered " + JSONUtil.serialize(this), null)
            return false
        }
        true
    }


    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "INSERT INTO ${space}.e_${suffix} (e, $colName, zv) VALUES (?, ?, ?)"
        if (value instanceof Date) { value = LocalDate.fromMillisSinceEpoch(((Date)value).time) }
        cqlargs = [
                docUUID,
                value,
                opctx.updateUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class DelFixedCol extends MutationCmd {
    String docUUID
    String colName

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "E" + docUUID, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("DelFixedCol filtered " + JSONUtil.serialize(this), null)
            return false
        }
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "UPDATE ${space}.e_${suffix} SET ${colName} = null WHERE e = ?"
        cqlargs = [docUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class ClrFixedCol extends DelFixedCol {
    boolean getClearCmd() { true }

    void batch(OperationContext opctx) {
        // clears are always in leftovers
        opctx.batchLeftovers.add(this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("ClrFixedCol filtered " + JSONUtil.serialize(this), null)
            return false
        }
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "UPDATE ${space}.e_${suffix} SET ${colName} = null WHERE e = ?"
        cqlargs = [docUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@Log
@CompileStatic
class UpdDocFixedColPAXOS extends MutationCmd {
    String docUUID
    String colName
    Object value
    UUID previousVersion

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "E" + docUUID, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("UpdDocFixedColPAXOS filtered " + JSONUtil.serialize(this), null)
            return false
        }
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        if (value instanceof Date) { value = LocalDate.fromMillisSinceEpoch(((Date)value).time) }
        cql = "INSERT INTO ${space}.e_${suffix} (e, $colName, zv) VALUES (?, ?, ?) IF zv = ?"
        cqlargs = [
                docUUID,
                value,
                opctx.updateUUID,
                previousVersion] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@Log
@CompileStatic
class NewAttr extends MutationCmd {
    String docUUID
    String attrName
    FieldValue attrValue
    boolean paxos = false

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "P" + docUUID, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("NewAttr filtered " + JSONUtil.serialize(this), null)
            return false
        }
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "INSERT INTO ${space}.p_${suffix} (e,p,zv,d,t) VALUES (?,?,?,?,?)" + (paxos ? " IF NOT EXIST" : "")
        cqlargs = [
                docUUID,
                attrName,
                opctx.updateUUID,
                attrValue?.value,
                TypeConfigurationService.attrTypeCode(attrValue?.type)] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@Log
@CompileStatic
class UpdAttr extends NewAttr {

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "P" + docUUID, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("UpdAttrCmd filtered " + JSONUtil.serialize(this), null)
            return false
        }
        if (opctx.deletedIdAttrs.contains(docUUID + attrName)) {
            log.dbg("UpdAttrCmd filtered from deleted attr " + JSONUtil.serialize(this), null)
            return false
        }
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "INSERT INTO ${space}.p_${suffix} (e,p,zv,d,t) VALUES (?,?,?,?,?)"
        cqlargs = [
                docUUID,
                attrName,
                opctx.updateUUID,
                attrValue?.value,
                TypeConfigurationService.attrTypeCode(attrValue?.type)] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }


}


@Log
@CompileStatic
class UpdAttrPAXOS extends UpdAttr {
    UUID previousVersion

    void batch(OperationContext opctx) {
        opctx.batchLeftovers.add(this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "UPDATE ${space}.p_${suffix} SET zv = ?, d = ?, t = ? WHERE e = ? and p = ? IF zv = ?"
        cqlargs = [
                opctx.updateUUID,
                attrValue?.value,
                TypeConfigurationService.attrTypeCode(attrValue?.type),
                docUUID,
                attrName,
                previousVersion] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}

@Log
@CompileStatic
class UpdDocMetadata extends MutationCmd {
    String docUUID
    String metadataUUID

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "E" + docUUID, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "UPDATE ${space}.e_${suffix} SET z_md = ? WHERE e = ?"
        cqlargs = [metadataUUID, docUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}

@Log
@CompileStatic
class UpdAttrMetadata extends MutationCmd {
    String docUUID
    String attr
    String metadataUUID

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "P" + docUUID, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "UPDATE ${space}.p_${suffix} SET z_md = ? WHERE e = ? AND p = ?"
        cqlargs = [metadataUUID, docUUID, attr] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}

@Log
@CompileStatic
class UpdRelMetadata extends MutationCmd {
    RelKey relkey
    String metadataUUID

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "R" + relkey.p1, this)
    }


    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "UPDATE ${space}.r SET z_md = ? WHERE p1 = ? AND ty1 = ? and ty2 = ? and ty3 = ? and ty4 = ? and p2 = ? and p3 = ? and p4 = ? and c1 = ? and c2 = ? and c3 = ? and c4 = ?"
        cqlargs = [
                metadataUUID,
                relkey.p1,
                relkey.ty1,
                relkey.ty2,
                relkey.ty3,
                relkey.ty4,
                relkey.p2,
                relkey.p3,
                relkey.p4,
                relkey.c1,
                relkey.c2,
                relkey.c3,
                relkey.c4] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@Log
@CompileStatic
class DelDoc_E extends MutationCmd {
    String docUUID

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "E" + docUUID, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        opctx.deletedIds.add(docUUID)
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "DELETE FROM ${space}.e_${suffix} WHERE e = ?"
        cqlargs = [docUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class DelDoc_P extends MutationCmd {
    String docUUID

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "P" + docUUID, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        opctx.deletedIds.add(docUUID)
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "DELETE FROM ${space}.p_${suffix} WHERE e = ?"
        cqlargs = [docUUID] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@Log
@CompileStatic
class DelAttr extends MutationCmd {
    String docUUID
    String attrName
    UUID previousVersion

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "P" + docUUID, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("DelAttr filtered " + JSONUtil.serialize(this), null)
            return false
        }
        opctx.deletedIdAttrs.add(docUUID + attrName)
        true
    }


    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "DELETE FROM ${space}.p_${suffix} WHERE e = ? and p = ?"
        cqlargs = [docUUID, attrName] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@CompileStatic
class ClrAttr extends DelAttr {
    boolean getClearCmd() { true }

    void batch(OperationContext opctx) {
        // clears are always in leftovers
        opctx.batchLeftovers.add(this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(docUUID)) {
            log.dbg("ClrAttr filtered " + JSONUtil.serialize(this), null)
            return false
        }
        if (opctx.deletedIdAttrs.contains(docUUID + attrName)) {
            return false
        }
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        String suffix = IDUtil.idSuffix(docUUID)
        cql = "DELETE FROM ${space}.p_${suffix} WHERE e = ? and p = ?"
        cqlargs = [docUUID, attrName] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@CompileStatic
class DelDocRels extends MutationCmd {
    String p1 = ""

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "R" + p1, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "DELETE FROM ${space}.r WHERE p1 = ?"
        cqlargs = [p1] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@CompileStatic
class ClrDocRels extends DelDocRels {
    boolean getClearCmd() { true }

    void batch(OperationContext opctx) {
        // clears are always in leftovers
        opctx.batchLeftovers.add(this)

    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(p1)) {
            log.dbg("ClrDocRels filtered " + JSONUtil.serialize(this), null)
            return false
        }
        return true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "DELETE FROM ${space}.r WHERE p1 = ?"
        cqlargs = [p1] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@CompileStatic
class DelAttrRels extends MutationCmd {
    String p1 = ""
    String ty1 = ""
    String p2 = ""

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "R" + p1, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(p1)) {
            log.dbg("DelAttrRels filtered " + JSONUtil.serialize(this), null)
            return false
        }
        return true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "DELETE FROM ${space}.r WHERE p1 = ? and ty1 = ? and ty2 = '' and ty3 = '' and ty4 = '' and p2 = ?"
        cqlargs = [p1, ty1, p2] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}

class DelRel extends MutationCmd {
    RelKey relKey

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "R" + relKey.p1, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(relKey.p1)) {
            log.dbg("DelAttrRels filtered " + JSONUtil.serialize(this), null)
            return false
        }
        return true
    }

    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "DELETE FROM ${space}.r WHERE p1 = ? and ty1 = ? and ty2 = ?  and ty3 = ? and ty4 = ? and p2 = ? and p3 = ? and p4 = ? and c1 = ? and c2 = ? and c3 = ? and c4 = ?"
        cqlargs = [
                relKey.p1,
                relKey.ty1,
                relKey.ty2,
                relKey.ty3,
                relKey.ty4,
                relKey.p2,
                relKey.p3,
                relKey.p4,
                relKey.c1,
                relKey.c2,
                relKey.c3,
                relKey.c4] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}


@CompileStatic
class ClrAttrRels extends DelAttrRels {
    boolean getClearCmd() { true }

    void batch(OperationContext opctx) {
        // clears are always in leftovers
        opctx.batchLeftovers.add(this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        if (opctx.deletedIds.contains(p1)) {
            log.dbg("ClrAttrRels filtered " + JSONUtil.serialize(this), null)
            return false
        }
        if (opctx.deletedIdAttrs.contains(p1 + p2)) {
            log.dbg("ClrAttrRels filtered for attr " + JSONUtil.serialize(this), null)
            return false
        }
        return true
    }


    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "DELETE FROM ${space}.r WHERE p1 = ? and ty1 = ? and ty2 = '' and ty3 = '' and ty4 = '' and p2 = ?"
        cqlargs = [p1, ty1, p2] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }
}


@CompileStatic
class NewRel extends MutationCmd {
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

    String link = ""
    String d = ""
    // TODO: paxos UUID versions

    void batch(OperationContext opctx) {
        ListMap.put(opctx.batches, "R" + p1, this)
    }

    boolean optimize(CommandExecServices svcs, OperationContext opctx, Detail detail) {
        return true
    }


    Object execMutationCassandra(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {
        space = opctx.space
        cql = "INSERT INTO ${space}.r (p1,ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4,link,d) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        cqlargs = [
                p1,
                ty1,
                ty2,
                ty3,
                ty4,
                p2,
                p3,
                p4,
                c1,
                c2,
                c3,
                c4,
                link,
                d] as Object[]
        return execOrPrep(svcs, opctx, detail)
    }

}

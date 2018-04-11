package cassdoc.operations

import groovy.transform.CompileStatic
import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.DocType
import cassdoc.FixedAttr
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelKey
import cassdoc.RelTypes
import cassdoc.commands.mutate.ClrAttr
import cassdoc.commands.mutate.ClrAttrRels
import cassdoc.commands.mutate.DelAttr
import cassdoc.commands.mutate.DelAttrRels
import cassdoc.commands.mutate.DelDocRels
import cassdoc.commands.mutate.DelDoc_E
import cassdoc.commands.mutate.DelDoc_P
import cassdoc.commands.mutate.DelFixedCol
import cassdoc.commands.mutate.DelRel
import cassdoc.commands.retrieve.GetAttrRelsCmd
import cassdoc.commands.retrieve.GetRelsCmd
import cassdoc.commands.retrieve.GetRelsRCH
import cwdrg.lg.annotation.Log

@Log
@CompileStatic
class DeleteOperations {

    // 1) determine clear vs delete process (timestamp only??) ... clear should have a different UUID!
    // 2) determine cleanup:
    // ---- relations will have subentities to (possibly) delete
    // ---- config will have custom indexes and fixed attribute cleanup
    // ---- detail can have special information (don't delete children of a certain type, etc)
    // ---- rels themselves


    static void deleteDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID) {
        log.inf("DELDOC_top:: $docUUID", null)
        GetRelsCmd getRels = new GetRelsCmd(p1: docUUID)
        GetRelsRCH docRels = getRels.queryCassandraDocRels(svcs, opctx, detail)

        analyzeDeleteDocEvent(svcs, opctx, detail, docUUID, docRels.rels)

    }

    static void deleteAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, boolean clear) {
        GetAttrRelsCmd getRels = new GetAttrRelsCmd(p1: docUUID, ty1s: [
                RelTypes.SYS_INDEX,
                RelTypes.TO_CHILD] as HashSet, p2: attr)
        GetRelsRCH attrChildDocs = getRels.queryCassandraAttrRels(svcs, opctx, detail, null)

        DelAttr delAttr = new DelAttr(docUUID: docUUID, attrName: attr)
        analyzeDeleteAttrEvent(svcs, opctx, detail, delAttr, attrChildDocs.rels, clear)

        // cascade the delete for child rels
        for (Rel childRel : attrChildDocs.rels) {
            if (childRel.ty1 == RelTypes.TO_CHILD) {
                String childDocUUID = childRel.c1
                // TODO: recurse/deletion cascade detail
                // TODO: recursive cleanup in a cleanup threadx

                deleteDoc(svcs, opctx, detail, childDocUUID)
            }
        }

    }

    static void analyzeDeleteDocEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, List<Rel> docRels) {
        DelDoc_E delDocE = new DelDoc_E(docUUID: docUUID)
        opctx.addCommand(svcs, detail, delDocE)

        // manual index cleanup
        IndexOperations.cleanupDocIndexes(svcs, opctx, detail, docUUID, docRels)

        DelDocRels delRels = new DelDocRels(p1: docUUID)
        opctx.addCommand(svcs, detail, delRels)

        DelDoc_P delDocP = new DelDoc_P(docUUID: docUUID)
        opctx.addCommand(svcs, detail, delDocP)

        for (Rel childRel : docRels) {
            if (childRel.ty1 == RelTypes.TO_CHILD) {
                String childDocUUID = childRel.c1
                //TODO: recurse detail
                deleteDoc(svcs, opctx, detail, childDocUUID)
            }
        }

        // TODO: clear parent rels to this UUID... or update? ... need to think about this.
        // TODO: clear bidirectional rels

    }

    static void analyzeDeleteAttrEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, DelAttr cmd, List<Rel> attrRels, boolean clear) {
        IndexOperations.cleanupDocAttrIndexes(svcs, opctx, detail, cmd.docUUID, cmd.attrName, attrRels)

        DocType docType = svcs.collections[opctx.space].first.getTypeForID(cmd.docUUID)
        FixedAttr fixed = docType.fixedAttrMap[cmd.attrName]

        if (fixed != null) {
            // clear isn't necessary here...
            DelFixedCol delFixedCol = new DelFixedCol(docUUID: cmd.docUUID, colName: fixed.colname)
        }

        if (clear) {
            ClrAttrRels clrSubDocRels = new ClrAttrRels(p1: cmd.docUUID, ty1: RelTypes.TO_CHILD, p2: cmd.attrName)
            opctx.addCommand(svcs, detail, clrSubDocRels)
            ClrAttrRels clrRelsIdx = new ClrAttrRels(p1: cmd.docUUID, ty1: RelTypes.SYS_INDEX, p2: cmd.attrName)
            opctx.addCommand(svcs, detail, clrRelsIdx)
            ClrAttr clrAttr = new ClrAttr(docUUID: cmd.docUUID, attrName: cmd.attrName)
            opctx.addCommand(svcs, detail, clrAttr)
        } else {
            DelAttrRels delRels = new DelAttrRels(p1: cmd.docUUID, ty1: RelTypes.TO_CHILD, p2: cmd.attrName)
            opctx.addCommand(svcs, detail, delRels)
            DelAttrRels delRelsIdx = new DelAttrRels(p1: cmd.docUUID, ty1: RelTypes.SYS_INDEX, p2: cmd.attrName)
            opctx.addCommand(svcs, detail, delRelsIdx)
            opctx.addCommand(svcs, detail, cmd)
        }

        // TODO: clear parent rels to this UUID... or update? ... need to think about this.
        // TODO: clear bidirectional rels

    }

    static void delRel(CommandExecServices svcs, OperationContext opctx, Detail detail, RelKey rel) {
        DelRel delRel = new DelRel(relKey: rel)
        opctx.addCommand(svcs, detail, delRel)
    }

}

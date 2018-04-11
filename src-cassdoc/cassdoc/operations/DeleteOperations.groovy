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
import cassdoc.commands.mutate.cassandra.MutationCmd
import cassdoc.commands.retrieve.RPUtil
import cassdoc.commands.retrieve.RowProcessor
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


  static void deleteDoc(CommandExecServices svcs,OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf( "DELDOC_top:: $docUUID",null)

    RowProcessor relCmd = svcs.retrievals.getDocRelsRP(docUUID)
    relCmd.initiateQuery(svcs, opctx, detail, null)
    List<Rel> rels = RPUtil.getAllRels(relCmd)
    analyzeDeleteDocEvent(svcs,opctx,detail,docUUID,rels)

  }


  static void deleteAttr(CommandExecServices svcs,OperationContext opctx, Detail detail, String docUUID, String attr, boolean clear)
  {
    RowProcessor relCmd = svcs.retrievals.getAttrRelsRP(docUUID,[
      RelTypes.SYS_INDEX,
      RelTypes.TO_CHILD
    ],attr)
    relCmd.initiateQuery(svcs, opctx, detail, null)
    List<Rel> rels = RPUtil.getAllRels(relCmd)

    analyzeDeleteAttrEvent(svcs,opctx,detail,docUUID,attr,,rels,clear)

    // cascade the delete for child rels
    for (Rel childRel : rels) {
      if (childRel.ty1 == RelTypes.TO_CHILD) {
        String childDocUUID = childRel.c1
        // TODO: recurse/deletion cascade detail
        // TODO: recursive cleanup in a cleanup threadx

        deleteDoc(svcs,opctx,detail,childDocUUID)
      }
    }


  }

  static void analyzeDeleteDocEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, List<Rel> docRels)
  {
    MutationCmd delDocE = svcs.mutations.delDocE(docUUID)
    opctx.addCommand(svcs, detail, delDocE)

    // manual index cleanup
    IndexOperations.cleanupDocIndexes(svcs, opctx, detail, docUUID, docRels)

    MutationCmd delRels = svcs.mutations.delDocRels(docUUID)
    opctx.addCommand(svcs, detail, delRels)

    MutationCmd delDocP = svcs.mutations.delDocP(docUUID)
    opctx.addCommand(svcs, detail, delDocP)


    for (Rel childRel : docRels) {
      if (childRel.ty1 == RelTypes.TO_CHILD) {
        String childDocUUID = childRel.c1
        //TODO: recurse detail
        deleteDoc(svcs,opctx,detail,childDocUUID)
      }
    }

    // TODO: clear parent rels to this UUID... or update? ... need to think about this.
    // TODO: clear bidirectional rels

  }

  static void analyzeDeleteAttrEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, List<Rel> attrRels, boolean clear)
  {
    IndexOperations.cleanupDocAttrIndexes(svcs, opctx, detail, docUUID, attrName, attrRels)

    DocType docType = svcs.typeSvc.getTypeForID(docUUID)
    FixedAttr fixed = docType.fixedAttrMap[attrName]

    if (fixed != null) {
      // clear isn't necessary here...
      // ... do we need a clear check here?
      MutationCmd delFixedCol = svcs.mutations.delFixedCol(docUUID,fixed.colname)
      opctx.addCommand(svcs, detail, delFixedCol) // ??? bugfix ???
    }

    if (clear) {
      MutationCmd clrSubDocRels = svcs.mutations.clrAttrRels(docUUID,RelTypes.TO_CHILD,attrName)
      opctx.addCommand(svcs, detail, clrSubDocRels)
      MutationCmd clrRelsIdx = svcs.mutations.clrAttrRels(docUUID,RelTypes.SYS_INDEX,attrName)
      opctx.addCommand(svcs, detail, clrRelsIdx)
      MutationCmd clrAttr = svcs.mutations.clrAttr(docUUID,attrName)
      opctx.addCommand(svcs, detail, clrAttr)
    } else {
      MutationCmd delRels = svcs.mutations.delAttrRels(docUUID,RelTypes.TO_CHILD,attrName)
      opctx.addCommand(svcs, detail, delRels)
      MutationCmd delRelsIdx = svcs.mutations.delAttrRels(docUUID,RelTypes.SYS_INDEX,attrName)
      opctx.addCommand(svcs, detail, delRelsIdx)
      opctx.addCommand(svcs, detail, svcs.mutations.delAttr(docUUID, attrName))
    }

    // TODO: clear parent rels to this UUID... or update? ... need to think about this.
    // TODO: clear bidirectional rels


  }

  static void delRel(CommandExecServices svcs, OperationContext opctx, Detail detail, RelKey rel)
  {
    MutationCmd delRel = svcs.mutations.delRel(rel)
    opctx.addCommand(svcs, detail, delRel)
  }



}

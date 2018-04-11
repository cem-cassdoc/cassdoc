package cassdoc.operations;

import groovy.transform.CompileStatic
import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.DocType
import cassdoc.FieldValue
import cassdoc.FixedAttr
import cassdoc.IDUtil
import cassdoc.IndexTypes
import cassdoc.ManualIndex
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelTypes
import cassdoc.commands.mutate.cassandra.ClrIdxVal
import cassdoc.commands.mutate.cassandra.InsIdxValOnly
import cassdoc.commands.mutate.cassandra.NewRel
import cassdoc.commands.mutate.cassandra.UpdFixedCol
import cwdrg.lg.annotation.Log
import cwdrg.util.json.JSONUtil

@CompileStatic
@Log
public class IndexOperations {



  static void cleanupDocIndexes(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, List<Rel> docRels) {
    DocType docType = svcs.typeSvc.getTypeForID(docUUID)

    Set<ManualIndex> processedIndexes = [] as Set
    for (Rel rel : docRels) {
      if (rel.ty1 == RelTypes.SYS_INDEX) {
        String idxRef = rel.c1
        ManualIndex idx = docType.indexMap[idxRef]
        if (idx in processedIndexes) {
          // debug msg: already cleaned up indexName
        } else {
          if (idx.indexType == IndexTypes.HAS_VALUE) {
            ClrIdxVal cmd = new ClrIdxVal()
            cmd.i1 = idx.indexCodes.size() > 0 ? idx.indexCodes[0] : ""
            cmd.i2 = idx.indexCodes.size() > 1 ? idx.indexCodes[1] : ""
            cmd.i3 = idx.indexCodes.size() > 2 ? idx.indexCodes[2] : ""
            cmd.k1 = rel.c2 != null ? rel.c2 : ""
            cmd.k2 = rel.c3 != null ? rel.c3 : ""
            cmd.k3 = rel.c4 != null ? rel.c4 : ""
            cmd.v1 = docUUID
            log.dbg("clean idx: "+JSONUtil.serialize(cmd),null)
            opctx.addCommand(svcs, detail, cmd)
          }
        }
      }
    }
  }

  static void cleanupDocAttrIndexes(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, List<Rel> attrRels) {
    DocType docType = svcs.typeSvc.getTypeForID(docUUID)

    log.dbg("docUUID: "+docUUID+" docType: "+docType?.suffix+" attrName: "+attrName,null)
    Set<ManualIndex> attrIndexes = docType.attrIndexMap[attrName]

    Set<ManualIndex> processedIndexes = [] as Set
    for (Rel rel : attrRels) {
      if (rel.ty1 == RelTypes.SYS_INDEX) {
        log.dbg("clean idx rel: "+JSONUtil.serialize(rel),null)
        String idxRef = rel.c1
        ManualIndex idx = docType.indexMap[idxRef]
        if (idx in processedIndexes) {
          // debug msg: already cleaned up indexName
        } else {
          if (idx.indexType == IndexTypes.HAS_VALUE) {
            ClrIdxVal cmd = new ClrIdxVal()
            cmd.i1 = idx.indexCodes.size() > 0 ? idx.indexCodes[0] : ""
            cmd.i2 = idx.indexCodes.size() > 1 ? idx.indexCodes[1] : ""
            cmd.i3 = idx.indexCodes.size() > 2 ? idx.indexCodes[2] : ""
            cmd.k1 = rel.c2 != null ? rel.c2 : ""
            cmd.k2 = rel.c3 != null ? rel.c3 : ""
            cmd.k3 = rel.c4 != null ? rel.c4 : ""
            cmd.v1 = docUUID
            log.dbg("clean idx: "+JSONUtil.serialize(cmd),null)
            opctx.addCommand(svcs, detail, cmd)
          }
        }
      }
    }
  }

  static void processNewAttrIndexes(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, FieldValue attrValue) {

    // fixed attr cols (basically these are indexes)
    String suffix = IDUtil.idSuffix(docUUID)
    DocType docType = svcs.typeSvc.getTypeForSuffix(suffix)
    FixedAttr attrdef = docType.fixedAttrMap[attrName]
    if (attrdef != null) {
      UpdFixedCol fixedcol = new UpdFixedCol(docUUID:docUUID,colName:attrdef.colname,value:CreateOperations.convertFieldValueToFixedColValue(attrValue, attrdef))
      opctx.addCommand(svcs, detail, fixedcol)
    }

    Set<ManualIndex> attrIndexes = docType.attrIndexMap[attrName]
    if (attrIndexes != null) {
      for (ManualIndex idx : attrIndexes) {
        // switch on implementing index type
        if (idx.indexType == IndexTypes.HAS_VALUE) {
          // TODO: handle value is null, ?which is a delete?

          // add docUUID to the index for this value

          InsIdxValOnly setHVIdx = new InsIdxValOnly(k1:attrValue?.value)
          setHVIdx.i1 = idx.indexCodes.size() > 0 ? idx.indexCodes[0] : ""
          setHVIdx.i2 = idx.indexCodes.size() > 1 ? idx.indexCodes[1] : ""
          setHVIdx.i3 = idx.indexCodes.size() > 2 ? idx.indexCodes[2] : ""
          setHVIdx.v1 = docUUID
          log.dbg("add idx: "+JSONUtil.serialize(setHVIdx),null)
          opctx.addCommand(svcs, detail, setHVIdx)

          // add "hasindex" to relations to avoid excessive reads on update/clear/delete
          NewRel rel = new NewRel()
          rel.p1 = docUUID
          rel.ty1 = RelTypes.SYS_INDEX
          rel.p2 = attrName
          rel.c1 = idx.indexRef
          rel.c2 = attrValue?.value
          log.dbg("add has-idx rel: "+JSONUtil.serialize(rel),null)
          opctx.addCommand(svcs, detail, rel)

        }
      }
    }
  }
}




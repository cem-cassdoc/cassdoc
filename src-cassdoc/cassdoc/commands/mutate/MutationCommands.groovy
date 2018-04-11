package cassdoc.commands.mutate;

import cassdoc.FieldValue
import cassdoc.Rel
import cassdoc.RelKey
import cassdoc.commands.mutate.cassandra.MutationCmd

public interface MutationCommands {
  MutationCmd newDoc(String docUUID, String parentUUID, String parentAttr);
  MutationCmd clrIdxVal(String i1, String i2, String i3, String k1, String k2, String k3, String v1, String v2, String v3);
  MutationCmd insIdxValOnly(String i1, String i2, String i3, String k1, String k2, String k3, String v1, String v2, String v3);
  MutationCmd updFixedCol(String docUUID, String colName, Object value);
  MutationCmd delFixedCol(String docUUID, String colName);
  MutationCmd clrFixedCol(String docUUID, String colName);
  MutationCmd updDocFixedColPAXOS(String docUUID, String colName, Object value, UUID previousVersion);
  MutationCmd newAttr(String docUUID, String attrName, FieldValue attrValue, boolean paxos);
  MutationCmd updAttr(String docUUID, String attrName, FieldValue attrValue);
  MutationCmd updAttrPAXOS(String docUUID, String attrName, FieldValue attrValue, UUID previousVersion, Object[] paxosId);
  MutationCmd updDocMetadata(String docUUID, String metadataUUID);
  MutationCmd updAttrMetadata(String docUUID, String attrName, String metadataUUID);
  MutationCmd updRelMetadata(RelKey relKey, String metadataUUID);
  MutationCmd delDocE(String docUUID);
  MutationCmd delDocP(String docUUID);
  MutationCmd delAttr(String docUUID, String attrName);
  MutationCmd clrAttr(String docUUID, String attrName);
  MutationCmd delDocRels(String docUUID);
  MutationCmd clrDocRels(String docUUID);
  MutationCmd delAttrRels(String docUUID, String relType, String attrName);
  MutationCmd clrAttrRels(String docUUID, String relType, String attrName);
  MutationCmd delRel(RelKey relKey);
  MutationCmd newRel(Rel r);
}
package cassdoc.commands.mutate.cassandra

import cassdoc.FieldValue
import cassdoc.Rel
import cassdoc.RelKey
import cassdoc.commands.mutate.MutationCommands

class CassandraMutationCommands implements MutationCommands {
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#newDoc(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd newDoc(String docUUID,String parentUUID, String parentAttr) {
    new NewDoc(docUUID:docUUID,parentUUID:parentUUID,parentAttr:parentAttr)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#clrIdxVal(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd clrIdxVal(String i1, String i2, String i3, String k1, String k2, String k3, String v1, String v2, String v3) {
    new ClrIdxVal(i1:i1,i2:i2,i3:i3,k1:k1,k2:k2,k3:k3,v1:v1,v2:v2,v3:v3)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#insIdxValOnly(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd insIdxValOnly(String i1, String i2, String i3, String k1, String k2, String k3, String v1, String v2, String v3) {
    new InsIdxValOnly(i1:i1,i2:i2,i3:i3,k1:k1,k2:k2,k3:k3,v1:v1,v2:v2,v3:v3)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updFixedCol(java.lang.String, java.lang.String, java.lang.Object)
   */
  @Override
  MutationCmd updFixedCol(String docUUID,String colName,Object value) {
    new UpdFixedCol(docUUID:docUUID,colName:colName,value:value)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delFixedCol(java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd delFixedCol(String docUUID,String colName) {
    new DelFixedCol(docUUID:docUUID,colName:colName)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#clrFixedCol(java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd clrFixedCol(String docUUID,String colName) {
    new ClrFixedCol(docUUID:docUUID,colName:colName)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updDocFixedColPAXOS(java.lang.String, java.lang.String, java.lang.Object, java.util.UUID)
   */
  @Override
  MutationCmd updDocFixedColPAXOS(String docUUID,String colName,Object value, UUID previousVersion) {
    new UpdFixedCol(docUUID:docUUID,colName:colName,value:value,previousVersion:previousVersion)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#newAttr(java.lang.String, java.lang.String, cassdoc.FieldValue, boolean)
   */
  @Override
  MutationCmd newAttr(String docUUID,String attrName,FieldValue attrValue, boolean paxos) {
    new NewAttr(docUUID:docUUID,attrName:attrName,attrValue:attrValue,paxos:paxos)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updAttr(java.lang.String, java.lang.String, cassdoc.FieldValue)
   */
  @Override
  MutationCmd updAttr(String docUUID,String attrName,FieldValue attrValue) {
    new UpdAttr(docUUID:docUUID,attrName:attrName,attrValue:attrValue)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updAttrPAXOS(java.lang.String, java.lang.String, cassdoc.FieldValue, java.util.UUID)
   */
  @Override
  MutationCmd updAttrPAXOS(String docUUID,String attrName,FieldValue attrValue,UUID previousVersion,Object[] paxosId) {
    new UpdAttrPAXOS(docUUID:docUUID,attrName:attrName,attrValue:attrValue,previousVersion:previousVersion,paxosId:paxosId)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updDocMetadata(java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd updDocMetadata(String docUUID,String metadataUUID) {
    new UpdDocMetadata(docUUID:docUUID,metadataUUID:metadataUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updAttrMetadata(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd updAttrMetadata(String docUUID,String attrName,String metadataUUID) {
    new UpdAttrMetadata(docUUID:docUUID,attr:attrName,metadataUUID:metadataUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#updRelMetadata(cassdoc.RelKey, java.lang.String)
   */
  @Override
  MutationCmd updRelMetadata(RelKey relKey,String metadataUUID) {
    new UpdRelMetadata(relKey:relKey,metadataUUID:metadataUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delDocE(java.lang.String)
   */
  @Override
  MutationCmd delDocE(String docUUID) {
    new DelDoc_E(docUUID:docUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delDocP(java.lang.String)
   */
  @Override
  MutationCmd delDocP(String docUUID) {
    new DelDoc_P(docUUID:docUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delAttr(java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd delAttr(String docUUID,String attrName) {
    new DelAttr(docUUID:docUUID,attrName:attrName)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#clrAttr(java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd clrAttr(String docUUID,String attrName) {
    new ClrAttr(docUUID:docUUID,attrName:attrName)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delDocRels(java.lang.String)
   */
  @Override
  MutationCmd delDocRels(String docUUID) {
    new DelDocRels(p1:docUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#clrDocRels(java.lang.String)
   */
  @Override
  MutationCmd clrDocRels(String docUUID) {
    new ClrDocRels(p1:docUUID)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delAttrRels(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd delAttrRels(String docUUID, String relType, String attrName) {
    new DelAttrRels(p1:docUUID,ty1:relType,p2:attrName)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#clrAttrRels(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  MutationCmd clrAttrRels(String docUUID, String relType, String attrName) {
    new ClrAttrRels(p1:docUUID,ty1:relType,p2:attrName)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#delRel(cassdoc.RelKey)
   */
  @Override
  MutationCmd delRel(RelKey relKey) {
    new DelRel(relKey:relKey)
  }
  /* (non-Javadoc)
   * @see cassdoc.commands.mutate.MutationCommands#NewRel(cassdoc.Rel)
   */
  @Override
  MutationCmd newRel(Rel r) {
    new NewRel(p1:r.p1,p2:r.p2,p3:r.p3,p4:r.p4,ty1:r.ty1,ty2:r.ty2,c1:r.c1,c2:r.c2,c3:r.c3,c4:r.c4,link:r.lk,d:r.d)
  }
}

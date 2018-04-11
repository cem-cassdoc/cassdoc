package cassdoc.commands.retrieve.cassandra

import groovy.transform.CompileStatic
import cassdoc.RelKey
import cassdoc.commands.retrieve.RetrievalCommands
import cassdoc.commands.retrieve.RowProcessor
import cwdrg.lg.annotation.Log


@CompileStatic
@Log
class CassandraRetrievalCommands implements RetrievalCommands {
  int a = 1;
  RowProcessor getDocRP(String docUUID) {
    new GetDocRP(docUUID:docUUID)
  }
  RowProcessor getDocAttrsRP(String docUUID) {
    new GetDocAttrsRP(docUUID:docUUID)
  }
  RowProcessor queryToListOfStrArrRP(String query) {
    new QueryToListOfStrArr(query:query)
  }
  RowProcessor queryToListOfObjArrRP(String query) {
    new QueryToListOfObjArr(query:query)
  }
  RowProcessor indexTableRP(String i1, String i2, String i3, String k1,String k2, String k3) {
    new IndexTableRP(i1:i1,i2:i2,i3:i3,k1:k1,k2:k2,k3:k3)
  }
  RowProcessor entityTableSecondaryIndexRP(String table, String column, String columnType, Object columnValue) {
    new EntityTableSecondaryIndexRP(table:table,column:column,columnType:columnType,columnValue:columnValue)
  }
  RowProcessor docAttrListRP(String docUUID) {
    new DocAttrListRP(docUUID:docUUID)
  }
  RowProcessor getAttrRP(String docUUID, String attrName) {
    new GetAttrRP(docUUID:docUUID,attrName:attrName)
  }
  RowProcessor getAttrMetaRP(String docUUID, String attrName) {
    new GetAttrMetaRP(docUUID:docUUID,attrName:attrName)
  }
  RowProcessor getAttrRelsRP(String p1, List<String> ty1s, String p2) {
    new GetAttrRelsRP(p1:p1,ty1s:ty1s,p2:p2)
  }
  RowProcessor getDocRelsRP(String p1) {
    new GetDocRelsRP(p1:p1)
  }
  RowProcessor getDocRelsForTypeRP(String p1, String ty1) {
    new GetDocRelsForTypeRP(p1:p1,ty1:ty1)
  }
  RowProcessor getAttrRelsForTypeRP(String p1, String ty1, String p2) {
    new GetAttrRelsForTypeRP(p1:p1,ty1:ty1,p2:p2)
  }
  RowProcessor getRelKeyRP(RelKey relkey) {
    new GetRelKeyRP(relKey:relkey)
  }
}

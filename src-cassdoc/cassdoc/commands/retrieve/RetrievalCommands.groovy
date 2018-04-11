



package cassdoc.commands.retrieve;

import cassdoc.RelKey

public interface RetrievalCommands {


  RowProcessor getDocAttrsRP(String docUUID)
  RowProcessor docAttrListRP(String docUUID) // are these the same thing?
  RowProcessor queryToListOfStrArrRP(String query)
  RowProcessor queryToListOfObjArrRP(String query)
  RowProcessor indexTableRP(String i1, String i2, String i3, String k1,String k2, String k3)
  RowProcessor entityTableSecondaryIndexRP(String table, String column, String columnType, Object columnValue)
  RowProcessor getDocRP(String docUUID)
  RowProcessor getAttrRP(String docUUID, String attrName)
  RowProcessor getAttrMetaRP(String docUUID, String attrName)
  RowProcessor getAttrRelsRP(String p1, List<String> ty1s, String p2)
  RowProcessor getDocRelsRP(String p1)
  RowProcessor getDocRelsForTypeRP(String p1, String ty1)
  RowProcessor getAttrRelsForTypeRP(String p1, String ty1, String p2)
  RowProcessor getRelKeyRP(RelKey relkey)
}
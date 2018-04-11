package cassdoc

import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringEscapeUtils
import org.springframework.beans.factory.annotation.Autowired

import cassdoc.commands.mutate.cassandra.UpdAttrMetadata;
import cassdoc.commands.mutate.cassandra.UpdDocMetadata;
import cassdoc.commands.mutate.cassandra.UpdRelMetadata;
import cassdoc.commands.retrieve.RowProcessor
import cassdoc.operations.CreateOperations
import cassdoc.operations.DeleteOperations
import cassdoc.operations.RetrievalOperations
import cassdoc.operations.UpdateOperations

import com.jayway.jsonpath.JsonPath

import cwdrg.lg.annotation.Log
import cwdrg.util.json.JSONUtil

/**
 * This is the primary API class. It has API methods for both JSON interactions and Map/List/Map.Entry interactions. 
 * 
 * 
 * @author cowardlydragon
 *
 */
@Log
@CompileStatic
class API {

  // TODO: cross-space registry (id : space) (id: space code + space codes

  @Autowired
  CommandExecServices svcs;


  boolean docExists(OperationContext opctx, Detail detail, String uuid)
  {
    // token would work too, at least on cass 3.5
    log.inf("DocExists :: $uuid",null)
    String typeSuffix = IDUtil.idSuffix(uuid)
    List<Object[]> rows = query(opctx,detail,"SELECT zv from ${opctx.space}.e_${typeSuffix} WHERE e = ?",uuid)
    boolean found = (rows.size() > 0)
    log.dbg("DocExists DONE :: $uuid :: $found",null)
    return found
  }

  boolean attrExists(OperationContext opctx, Detail detail, String uuid, String attr)
  {
    log.inf("AttrExists :: $uuid $attr",null)
    String typeSuffix = IDUtil.idSuffix(uuid)
    List<Object[]> rows = query(opctx,detail,"SELECT zv from ${opctx.space}.p_${typeSuffix} WHERE e = ? and p = ?",uuid,attr)
    boolean found = (rows.size() > 0)
    log.dbg("AttrExists DONE :: $uuid $attr :: $found",null)
    return found
  }

  // ---- Streaming Parse Read Operations

  Iterator<String> getAttrNamesIterator(OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf("GetAttrNamesIterator :: $docUUID",null)
    Iterator<String> attrNames = RetrievalOperations.attrNamesIterator(svcs, opctx, detail, docUUID)
    return attrNames
  }

  void getAttrNames(OperationContext opctx, Detail detail, String docUUID, Writer writer)
  {
    log.inf("GetAttrNames :: $docUUID",null)
    writer << '['
    Iterator<String> names = getAttrNamesIterator(opctx,detail,docUUID)

    boolean first = true
    for (String name : names) {
      if (first) first = false else writer << ','
      writer << '"' << StringEscapeUtils.escapeJson(name) << '"'
    }
    writer << ']'
    log.dbg("GetAttrNames DONE :: $docUUID",null)
  }

  /**
   * Get a document attribute, with no recursion or document parsing introspection for subdocuments.
   * 
   * Non-streaming version.
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @return
   */
  String getSimpleAttr(OperationContext opctx, Detail detail, String docUUID, String attr)
  {
    log.inf("GetSimpleAttr :: $docUUID $attr",null)
    StringWriter writer = new StringWriter()
    getSimpleAttr(opctx,detail,docUUID,attr,writer)
    String toStr = writer.toString()
    log.dbg("GetSimpleAttr DONE :: $docUUID $attr :: "+toStr,null)
    return toStr
  }

  /**
   * Get a document attribute, with no recursion or document parsing introspection for subdocuments.
   * 
   * Streaming version.
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param writer
   */
  void getSimpleAttr (OperationContext opctx, Detail detail, String docUUID, String attr, Writer writer)
  {
    log.inf("GetSimpleAttr[wrt] :: $docUUID $attr",null)
    RowProcessor cmd = svcs.retrievals.getAttrRP(docUUID, attr)
    cmd.initiateQuery(svcs,opctx,detail,null)
    Object[] row = cmd.nextRow()
    if (row[0] == DBCodes.TYPE_CODE_STRING) {
      writer << '"' << StringEscapeUtils.escapeJson(row[1].toString()) << '"'
    } else {
      writer << row[1]
    }
    log.dbg("GetSimpleAttr[wrt] DONE :: $docUUID $attr",null)
  }

  Object deserializeSimpleAttr(OperationContext opctx, Detail detail, String docUUID, String attr)
  {
    log.inf("DeserializeSimpleAttr :: $docUUID $attr",null)
    RowProcessor cmd = svcs.retrievals.getAttrRP(docUUID,attr)
    cmd.initiateQuery(svcs,opctx,detail,null)
    Object[] row = cmd.nextRow()
    log.dbg("DeserializeSimpleAttr DONE :: $docUUID $attr :: "+row[1],null)
    if (row[1] == null) {
      return null
    }
    if (row[0] == DBCodes.TYPE_CODE_OBJECT) {
      return JSONUtil.deserializeMap(row[1].toString())
    }
    if (row[0] == DBCodes.TYPE_CODE_ARRAY) {
      return JSONUtil.deserializeList(row[1].toString())
    }
    if (row[0] == DBCodes.TYPE_CODE_STRING) {
      return row[1].toString()
    }
    if (row[0] == DBCodes.TYPE_CODE_BOOLEAN) {
      return Boolean.parseBoolean(row[1].toString())
    }
    if (row[0] == DBCodes.TYPE_CODE_INTEGER) {
      return new BigInteger(row[1].toString())
    }
    if (row[0] == DBCodes.TYPE_CODE_DECIMAL) {
      return new BigDecimal(row[1].toString())
    }
    return null
  }

  /**
   * Get a document, with no recursion for subdocuments or other parsing of the document's content.
   * 
   * Non-streaming version
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @return
   */
  String getSimpleDoc(OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf("GetSimpleDoc :: $docUUID",null)
    StringWriter writer = new StringWriter()
    getSimpleDoc(opctx,detail,docUUID,writer)
    String doc = writer.toString()
    log.dbg("GetSimpleDoc DONE :: $docUUID :: "+doc,null)
    return doc
  }

  /**
   * Get a document, with no recursion for subdocuments or other parsing of the document's content.
   * 
   * Streaming version
   * 
   * TODO: the attribute pulls are not streaming based on a rolling result set however....fix this
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param writer
   */
  void getSimpleDoc (OperationContext opctx, Detail detail, String docUUID, Writer writer)
  {
    log.inf("GetSimpleDoc[wrt] :: $docUUID",null)
    writer << '{"_id":"'<<docUUID<<'"'

    RowProcessor cmd = svcs.retrievals.getDocAttrsRP(docUUID)
    cmd.initiateQuery(svcs,opctx,detail)
    Object[] attr = null
    while (attr = cmd.nextRow()) {
      writer << ',"'<< StringEscapeUtils.escapeJson((String)attr[0]) << '":'
      if (attr[1] == DBCodes.TYPE_CODE_STRING) {
        writer << '"' << StringEscapeUtils.escapeJson((String)attr[2]) << '"'
      } else {
        writer << attr[2]
      }

    }
    writer << '}'
    log.dbg("GetSimpleDoc[wrt] DONE :: $docUUID",null)
  }

  /**
   * Get a document, with parsing of document content for recursive subdocument pulls if inidcated by detail
   * 
   * Non-streaming version
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @return
   */
  String getDoc(OperationContext opctx, Detail detail, String docUUID) {
    log.inf("GetDoc :: $docUUID",null)
    StringWriter writer = new StringWriter()
    RetrievalOperations.getSingleDoc(svcs,opctx,detail,docUUID,writer,true)
    String doc = writer.toString()
    log.dbg("GetDoc DONE :: $docUUID :: "+doc,null)
    return doc
  }

  /**
   * Get a document, with parsing of document content for recursive subdocument pulls if inidcated by detail
   * 
   * Streaming version
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param writer
   */
  void getDoc(OperationContext opctx, Detail detail, String docUUID, Writer writer) {
    log.inf("GetDoc[wrt] :: $docUUID",null)
    RetrievalOperations.getSingleDoc(svcs,opctx,detail,docUUID,writer,true)
    log.dbg("GetDoc[wrt] DONE :: $docUUID",null)
  }

  /**
   * Get a document deserialized into a Map
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @return
   */
  Map<String,Object> deserializeDoc(OperationContext opctx, Detail detail, String docUUID) {
    log.inf("DeserializeDoc :: $docUUID",null)
    Map map = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, docUUID, true)
    log.dbg("DeserializeDoc DONE :: $docUUID :: "+JSONUtil.serialize(map),null)
    return map
  }

  // TODO: JsonPath for getDoc

  /**
   * Get a document attribute, with parsing of attribute's content for recursive subdocument pulls if inidcated by detail
   * 
   * Non-streaming version
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @return
   */
  String getAttr(OperationContext opctx, Detail detail, String docUUID, String attr) {
    log.inf("GetAttr :: $docUUID $attr",null)
    StringWriter writer = new StringWriter()
    RetrievalOperations.getAttr(svcs,opctx,detail,docUUID,attr,writer)
    String attrval = writer.toString()
    log.dbg("GetAttr DONE :: $docUUID $attr :: "+attrval,null)
    return attrval
  }

  /**
   * Get a document attribute, with parsing of attribute's content for recursive subdocument pulls if inidcated by detail
   * 
   * Streaming version
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param writer
   */
  void getAttr(OperationContext opctx, Detail detail, String docUUID, String attr, Writer writer) {
    log.inf("GetAttr[wrt] :: $docUUID $attr",null)
    RetrievalOperations.getAttr(svcs,opctx,detail,docUUID,attr,writer)
    log.dbg("GetAttr[wrt] DONE :: $docUUID $attr",null)
  }

  /**
   * Get a document attribute, with parsing of attribute's content for recursive subdocument pulls if inidcated by detail
   *
   * Non-streaming version
   *
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @return
   */
  Object deserializeAttr(OperationContext opctx, Detail detail, String docUUID, String attr) {
    log.inf("DeserializeAttr :: $docUUID $attr",null)
    Object attrVal =  RetrievalOperations.deserializeAttr(svcs, opctx, detail, docUUID, attr)
    log.dbg("DeserializeAttr DONE :: $docUUID $attr :: "+JSONUtil.serialize(attrVal),null)
    return attrVal
  }


  // TODO: jsonpath - there appear to be mutation abilities as well
  // TODO: jsonpath - avoid full serialization step... would require a custom json

  /**
   * execute the provided jsonpath expression against the json representation of the requested document
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param jsonPath
   * @return
   */
  String getDocJsonPath(OperationContext opctx, Detail detail, String docUUID, String jsonPath) {
    log.inf("GetDocJsonPath :: $docUUID $jsonPath",null)
    StringWriter writer = new StringWriter()
    RetrievalOperations.getSingleDoc(svcs,opctx,detail,docUUID,writer,true)
    String json = writer.toString()
    JsonPath pathexpr = JsonPath.compile(jsonPath)
    String result = JsonPath.parse(json).read(pathexpr).toString()
    log.dbg("GetDocJsonPath DONE :: $docUUID $jsonPath :: "+result,null)
    return result
  }

  /**
   * execute the provided jsonpath expression against the json value of the attribute
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param jsonPath
   * @return
   */
  String getAttrJsonPath(OperationContext opctx, Detail detail, String docUUID, String attr, String jsonPath) {
    //http://blog.ostermiller.org/convert-a-java-writer-to-a-reader/
    log.inf("GetAttrJsonPath :: $docUUID $attr $jsonPath",null)
    StringWriter writer = new StringWriter()
    RetrievalOperations.getAttr(svcs,opctx,detail,docUUID,attr,writer)
    String json =  writer.toString()
    JsonPath pathexpr = JsonPath.compile(jsonPath)
    String result = JsonPath.parse(json).read(pathexpr).toString()
    log.dbg("GetAttrJsonPath DONE :: $docUUID $attr $jsonPath :: "+result,null)
    return result
  }

  /**
   * Delete document: cascading deletes of subdocuments are controlled by detail. In cassandra this should delete the entire row and it's relations
   * 
   * threaded = false will synchronously delete it
   * 
   * threaded = true will delegate the cleanup to a thread and quick-return 
   *  
   * @param opctx
   * @param detail
   * @param docUUID
   */
  void delDoc(OperationContext opctx, Detail detail, String docUUID, boolean threaded)
  {
    if (threaded) {
      new Thread () {
            public void run() {
              log.inf("DelDoc[async] :: $docUUID",null)
              DeleteOperations.deleteDoc(svcs, opctx, detail, docUUID)
              if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
              log.dbg("DelDoc[async] DONE :: $docUUID",null)
            }
          }.start()
    } else {
      log.inf("DelDoc :: $docUUID",null)
      DeleteOperations.deleteDoc(svcs, opctx, detail, docUUID)
      if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
      log.dbg("DelDoc DONE :: $docUUID",null)
    }
  }

  /**
   * Delete document attribute: cascading deletes of the attribute's subdocuments are controlled by detail. In cassandra this deletes a column key within a row
   *
   * threaded = false will synchronously delete the attribute
   * 
   * threaded = true will delegate the delete to a thread and quick-return 
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   */
  void delAttr(OperationContext opctx, Detail detail, String docUUID, String attr, boolean threaded)
  {
    if (threaded) {
      new Thread () {
            public void run() {
              log.inf("DelAttr[async] $docUUID $attr",null)
              DeleteOperations.deleteAttr(svcs, opctx, detail, docUUID, attr, false)
              if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
              log.dbg("DelAttr[async] DONE $docUUID $attr",null)
            }
          }.start()
    } else {
      log.inf("DelAttr $docUUID $attr",null)
      DeleteOperations.deleteAttr(svcs, opctx, detail, docUUID, attr, false)
      if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
      log.dbg("DelAttr DONE $docUUID $attr",null)
    }
  }

  // ---- Streaming Parse Write Operations

  /**
   * Create new document from the provided map. Subobject maps inside the map will become child docs
   * if the subobject has the id field in it. No implicit ordering of the keys so that the id field 
   * is first is necessary as with maps, as opposed to the json parsing
   *
   * The _id is autogenerated based on the indicated document type. If a fully pregenerated _id value is provided in the JSON, it is discarded since that is unreliable / insecure
   *
   * @param opctx
   * @param detail
   * @param json
   * @return
   */
  public String newDocFromMap(OperationContext opctx, Detail detail, Map<String,Object> mapDoc, boolean threaded) {
    log.inf("NewDocFromMap",null)
    String newid =  CreateOperations.newMap(svcs,opctx,detail,mapDoc,threaded)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("NewDocFromMap DONE :: $newid",null)
    return newid
  }


  /**
   * Create new document from the provided JSON string. 
   * 
   * The _id is autogenerated based on the indicated document type. If a fully pregenerated _id value is provided in the JSON, it is discarded since that is unreliable / insecure
   * 
   * @param opctx
   * @param detail
   * @param json
   * @return
   */
  public String newDoc(OperationContext opctx, Detail detail, String json) {
    log.inf("NewDoc",null)
    String newid =  CreateOperations.newDoc(svcs,opctx,detail,json,false)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("NewDoc DONE :: $newid",null)
    return newid
  }

  /**
   * Create new document from the provided JSON reader.
   *
   * The _id is autogenerated based on the indicated document type. If a fully pregenerated _id value is provided in the JSON, it is discarded since that is unreliable / insecure
   *
   * @param opctx
   * @param detail
   * @param json
   * @return
   */
  public String newDoc(OperationContext opctx, Detail detail, Reader json) {
    log.inf("NewDoc",null)
    String newid =  CreateOperations.newDoc(svcs,opctx,detail,json,false)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("NewDoc DONE :: $newid",null)
    return newid
  }

  /**
   * Create new document from the provided JSON reader, returning the id as soon as generated and 
   * delegating the remaining document parsing and mutations to a background thread.
   * 
   * There is no callback/completion signal when the background thread has finished.  
   *
   * The _id is autogenerated based on the indicated document type. If a fully pregenerated _id value is provided in the JSON, it is discarded since that is unreliable / insecure
   *
   * @param opctx
   * @param detail
   * @param json
   * @return
   */
  public String newDocAsync(OperationContext opctx, Detail detail, Reader json) {
    log.inf("NewDocAsync",null)
    String newid =  CreateOperations.newDoc(svcs,opctx,detail,json,true)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("NewDocAsync DONE :: $newid",null)
    return newid
  }



  /**
   * A streaming multiple document call. Input stream is a json array of documents (initiated by a root '[' start array character and ended by the matching ']' end array character.
   * 
   * Documents are returned in a streaming JSON array of strings corresponding to the generated IDs.
   * 
   * @param opctx
   * @param detail
   * @param jsonListReader
   * @param jsonList
   */
  public void newDocList(OperationContext opctx, Detail detail, Reader jsonListReader, Writer jsonIDList)
  {
    log.inf("NewDocList",null)
    CreateOperations.newDocStream(svcs, opctx, detail, jsonListReader, jsonIDList)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
    log.dbg("NewDocList DONE",null)
  }


  /**
   * Add a new attribute to a document, verifying that the attribute does not already exist.
   * 
   * Synchronous TODO: async
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param json
   * @param paxos - if true, IF NOT EXIST paxos conditional is applied to upsert statement
   */
  public void newAttr(OperationContext opctx, Detail detail, String docUUID, String attr, String json, boolean paxos, boolean threaded)
  {
    if (threaded) {
      new Thread() {
            public void run() {
              log.inf("NewAttr[async] :: $docUUID $attr",null)
              CreateOperations.newAttr(svcs,opctx,detail,docUUID,attr,json, paxos)
              if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
              log.dbg("NewAttr[async] DONE :: $docUUID $attr",null)
            }
          }.start()
    } else {
      log.inf("NewAttr :: $docUUID $attr",null)
      CreateOperations.newAttr(svcs,opctx,detail,docUUID,attr,json, paxos)
      if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
      log.dbg("NewAttr DONE :: $docUUID $attr",null)
    }
  }


  // ---- Some Update operations

  /**
   * Update the attribute of a document using PAXOS on the document. This requires that the invoker
   * has the expected version/checkvalue UUID already from a previous retrieval. Note that PAXOS is 
   * not employed currently for cascading deletes/updates that result, instead the initial update to 
   * the attribute "p" table is considered the table of record, and resulting operations from that 
   * update are performed as cleanup. 
   * 
   * Synchronous
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param json
   * @param checkVal
   */
  public void updateAttrPAXOS(OperationContext opctx, Detail detail, String docUUID, String attr, String json, UUID checkVal)
  {
    log.inf("UpdateAttrPAXOS :: $docUUID $attr $checkVal",null)
    opctx.paxosGatekeeperUpdateID = ["P", docUUID] as String[]
    UpdateOperations.updateAttrPAXOS(svcs,opctx,detail,docUUID,attr,json,checkVal)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
    log.dbg("UpdateAttrPAXOS DONE :: $docUUID $attr $checkVal",null)
  }

  /**
   * Update the attribute of a document, using only detail-indicated consistency indicators and NOT using PAXOS. 
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param json
   */
  public void updateAttr(OperationContext opctx, Detail detail, String docUUID, String attr, String json, boolean threaded)
  {
    if (threaded) {
      new Thread() {
            public void run() {
              log.inf("UpdateAttr[async] :: $docUUID $attr",null)
              UpdateOperations.updateAttr(svcs,opctx,detail,docUUID,attr,json)
              if (opctx.executionMode == "batch")     opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
              log.dbg("UpdateAttr[async] DONE :: $docUUID $attr",null)
            }
          }.start()
    } else {
      log.inf("UpdateAttr :: $docUUID $attr",null)
      UpdateOperations.updateAttr(svcs,opctx,detail,docUUID,attr,json)
      if (opctx.executionMode == "batch")     opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
      log.dbg("UpdateAttr DONE :: $docUUID $attr",null)
    }
  }

  /**
   * Update the attribute of a document, using only detail-indicated consistency indicators and NOT using PAXOS.
   * 
   * This is the non-JSON API version
   *
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param json
   */
  public void updateAttrEntry(OperationContext opctx, Detail detail, String docUUID, Map.Entry<String,Object> attr, boolean threaded)
  {
    if (threaded) {
      new Thread() {
            public void run() {
              log.inf("UpdateAttrEntry[async] :: $docUUID $attr",null)
              UpdateOperations.updateAttrEntry(svcs,opctx,detail,docUUID,attr)
              if (opctx.executionMode == "batch")     opctx.DO(svcs, detail)
              log.dbg("UpdateAttrEntry[async] DONE :: $docUUID $attr",null)
            }
          }.start()
    } else {
      log.inf("UpdateAttrEntry :: $docUUID $attr",null)
      UpdateOperations.updateAttrEntry(svcs,opctx,detail,docUUID,attr)
      if (opctx.executionMode == "batch")     opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
      log.dbg("UpdateAttrEntry DONE :: $docUUID $attr",null)
    }
  }


  /**
   * This updates a document with mixed creation of new subdocuments as well as preserving indicated extant ids. 
   * Subdocuments with _ids that are blank / unformed are created as new documents
   * Subdocuments with fully formed _ids are assumed to currently exist
   * Existing subdocuments that aren't encountered in the overlay update are removed/cleanedup
   * 
   *  Synchronous
   * 
   * @param opctx
   * @param detail
   * @param docUUID
   * @param attr
   * @param json
   * @return
   */
  public Set<String> updateAttrOverlay(OperationContext opctx, Detail detail,String docUUID, String attr, String json, boolean threaded)
  {
    if (threaded) {
      new Thread() {
            public void run() {
              log.inf("UpdateAttrOverlay[async] :: $docUUID $attr",null)
              UpdateOperations.updateAttrOverlay(svcs, opctx, detail, docUUID, attr, json)
              if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
              log.dbg("UpdateAttrOverlay[async] DONE :: $docUUID $attr",null)
            }
          }.start()
    } else {
      log.inf("UpdateAttrOverlay :: $docUUID $attr",null)
      UpdateOperations.updateAttrOverlay(svcs, opctx, detail, docUUID, attr, json)
      if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
      log.dbg("UpdateAttrOverlay DONE :: $docUUID $attr",null)
    }
  }

  public List<Object[]> query(OperationContext opctx, Detail detail, String cql, Object[] args)
  {
    log.inf("Query :: $cql :: "+JSONUtil.serialize(args),null)
    RowProcessor cmd = svcs.retrievals.queryToListOfStrArrRP(cql)
    if (args != null)
      cmd.initiateQuery(svcs, opctx, detail, args)
    else
      cmd.initiateQuery(svcs, opctx, detail)
    List queryresult = []
    Object[] data = null
    while (data = cmd.nextRow()) {
      queryresult.add(data)
    }
    log.dbg("Query DONE :: $cql :: "+JSONUtil.serialize(args),null)
    return queryresult
  }

  public String getDocMetadata(OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf("GetDocMetadata :: $docUUID ",null)
    Writer writer = new StringWriter()
    String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    String data =  writer.toString()
    log.dbg("GetDocMetadata DONE :: $docUUID :: "+data,null)
    return data
  }

  public void getDocMetadata(OperationContext opctx, Detail detail, String docUUID, Writer writer)
  {
    log.inf("GetDocMetadata[wrt] :: $docUUID ",null)
    String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    log.dbg("GetDocMetadata[wrt] DONE :: $docUUID",null)
  }

  public Map<String,Object> deserializeDocMetadata(OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf("DeserializeDocMetadata :: $docUUID ",null)
    String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
    Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, metaid, true)
    log.dbg("DeserializeDocMetadata DONE :: $docUUID :: "+JSONUtil.serialize(doc),null)
    return doc
  }


  public String getAttrMetadata(OperationContext opctx, Detail detail, String docUUID, String attr)
  {
    log.inf("GetAttrMetadata :: $docUUID $attr",null)
    Writer writer = new StringWriter()
    String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    String data = writer.toString()
    log.dbg("GetAttrMetadata DONE :: $docUUID :: "+data,null)
    return data
  }

  public void getAttrMetadata(OperationContext opctx, Detail detail, String docUUID, String attr, Writer writer)
  {
    log.inf("GetAttrMetadata[wrt] :: $docUUID $attr",null)
    String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    log.dbg("GetAttrMetadata[wrt] DONE :: $docUUID $attr",null)
  }

  public Map<String,Object> deserializeAttrMetadata(OperationContext opctx, Detail detail, String docUUID, String attr)
  {
    log.inf("DeserializeAttrMetadata :: $docUUID $attr",null)
    String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
    Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, metaid, true)
    log.dbg("DeserializeAttrMetadata DONE :: $docUUID $attr :: "+JSONUtil.serialize(doc),null)
    return doc
  }


  public String getRelMetadata(OperationContext opctx, Detail detail, RelKey rel)
  {
    log.inf("GetRelMetadata :: "+JSONUtil.serialize(rel),null)
    Writer writer = new StringWriter()
    String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    String data =  writer.toString()
    log.dbg("GetRelMetadata DONE :: "+JSONUtil.serialize(rel)+" :: "+data,null)
    return data
  }

  public void getRelMetadata(OperationContext opctx, Detail detail, RelKey rel, Writer writer)
  {
    log.inf("GetRelMetadata[wrt] :: "+JSONUtil.serialize(rel),null)
    String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    log.dbg("GetRelMetadata[wrt] DONE :: "+JSONUtil.serialize(rel),null)
  }

  public Map<String,Object> deserializeRelMetadata(OperationContext opctx, Detail detail, RelKey rel)
  {
    log.inf("DeserializeRelMetadata :: "+JSONUtil.serialize(rel),null)
    String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
    Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, metaid, true)
    log.dbg("DeserializeRelMetadata DONE :: "+JSONUtil.serialize(rel)+" :: "+JSONUtil.serialize(doc),null)
    return doc
  }


  public String docMetadataUUID(OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf("DocMetadataUUID :: $docUUID",null)
    String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
    if (metaid != null) {
      return metaid
    }
    // create new UUID for META
    Detail initDetail = new Detail()
    initDetail.writeConsistency = detail.readConsistency
    initDetail.docMetaIDMeta = true
    metaid = CreateOperations.newDoc(svcs, opctx, initDetail, '{"_id":"META"}', false)
    UpdDocMetadata upd = new UpdDocMetadata(docUUID:docUUID, metadataUUID: metaid)
    opctx.addCommand(svcs, detail, upd)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("DocMetadataUUID DONE :: $docUUID :: $metaid",null)
    return metaid
  }


  public String attrMetadataUUID(OperationContext opctx, Detail detail, String docUUID, String attr)
  {
    log.inf("AttrMetadataUUID :: $docUUID $attr",null)
    String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
    if (metaid != null) {
      return metaid
    }
    // create new UUID for META
    metaid = CreateOperations.newDoc(svcs, opctx, detail, '{"_id":"META"}', false)
    UpdAttrMetadata upd = new UpdAttrMetadata(docUUID:docUUID,attr:attr, metadataUUID: metaid)
    opctx.addCommand(svcs, detail, upd)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("AttrMetadataUUID DONE :: $docUUID $attr :: $metaid",null)
    return metaid
  }

  public String relMetadataUUID(OperationContext opctx, Detail detail, RelKey rel)
  {
    log.inf("RelMetadataUUID :: "+JSONUtil.serialize(rel),null)
    String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
    if (metaid != null) {
      return metaid
    }
    // create doc + relation
    metaid = CreateOperations.newDoc(svcs, opctx, detail, '{"_id":"META"}', false)
    // Update the z_md field in the e table for the doc TODO: should be paxos...
    UpdRelMetadata upd = new UpdRelMetadata(relkey:rel, metadataUUID: metaid)
    opctx.addCommand(svcs, detail, upd)
    if (opctx.executionMode == "batch") opctx.DO(svcs, detail)
    log.dbg("RelMetadataUUID DONE :: "+JSONUtil.serialize(rel)+" :: $metaid",null)
    return metaid
  }

  public Rel deserializeRel(OperationContext opctx, Detail detail, RelKey relkey)
  {
    log.inf("DeserializeRel :: "+JSONUtil.serialize(relkey),null)
    Rel rel = RetrievalOperations.getRel(svcs, opctx, detail, relkey)
    log.dbg("DeserializeRel DONE :: "+JSONUtil.serialize(rel),null)
    return rel
  }

  /**
   * this is executed as an upsert, so this can be used to update the few non-key fields of a Rel as well
   * 
   * @param opctx
   * @param detail
   * @param rel
   */
  public void addRel(OperationContext opctx, Detail detail, Rel rel)
  {
    log.inf("AddRel :: "+JSONUtil.serialize(rel),null)
    CreateOperations.addRel(svcs, opctx, detail, rel)
    if (opctx.executionMode == "batch")opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
    log.dbg("AddRel DONE :: "+JSONUtil.serialize(rel),null)
  }

  public void deleteRel(OperationContext opctx, Detail detail, RelKey rel)
  {
    log.inf("DeleteRel :: "+JSONUtil.serialize(rel),null)
    DeleteOperations.delRel(svcs, opctx, detail, rel)
    if (opctx.executionMode == "batch")opctx.DO(svcs, detail) // TODO: figure out this vs streaming data operations
    log.dbg("DeleteRel DONE :: "+JSONUtil.serialize(rel),null)
  }

  public List<Rel> deserializeDocRels(OperationContext opctx, Detail detail, String docUUID)
  {
    log.inf("DeserializeDocRels :: $docUUID",null)
    List<Rel> rels = RetrievalOperations.deserializeDocRels(svcs,opctx,detail,docUUID)
    log.dbg("DeserializeDocRels DONE :: $docUUID :: "+JSONUtil.serialize(rels),null)
    return rels
  }



  /**
   * Searches for large distributed databases should be done via indexes, that are registered/known to the engine.
   *
   *  Index types: secondary indexes (cassandra maintained), materialized views (cass maintained), manual value indexes, external indexes (B+ in relational store)
   *
   *  TODO: sorter
   *
   * @param indexName
   * @return
   */
  public Iterator<Map> searchIndex(OperationContext opctx, Detail detail, String indexName, List searchCriteria, List<SearchFilter> filters)
  {
    log.inf("SearchIndex :: $indexName "+JSONUtil.serialize(searchCriteria),null)
    Index idx = svcs.idxSvc.getIndex(indexName)
    Iterator<Map> iterator = idx.searchIndex(svcs, opctx, detail, searchCriteria)
    return iterator
  }


  /**
   * Searches for large distributed databases should be done via indexes, that are registered/known to the engine.
   *
   *  Index types: secondary indexes (cassandra maintained), materialized views (cass maintained), manual value indexes, external indexes (B+ in relational store)
   *  
   *  TODO: sorter
   *
   * @param indexName
   * @return
   */
  public void searchIndex(OperationContext opctx, Detail detail, String indexName, List searchCriteria, List<SearchFilter> filters, Writer searchResultsWriter)
  {
    log.inf("SearchIndex[wrt] :: $indexName "+JSONUtil.serialize(searchCriteria),null)
    Iterator<Map> iterator = searchIndex(opctx,detail,indexName, searchCriteria, filters)
    searchResultsWriter << "["
    while (iterator.hasNext()) {
      Map doc = iterator.next()
      searchResultsWriter << "{" << '"_id":'
      searchResultsWriter << doc._id
      for (Map.Entry e : doc.entrySet()) {
        if (e.key != "_id") {
          searchResultsWriter << ',"' << StringEscapeUtils.escapeJson(e.key.toString()) << '":'
          searchResultsWriter << JSONUtil.serialize(e.value)
        }
      }
      searchResultsWriter << "}"
    }
  }

}

// PAXOS...
// - paxos updates can be batched if rowkey/table aka partition is shared:
// ... each unsafe update is a random negative version number
// ... paxos will update with a positive incremental
// ... store last paxos version too, so paxos re-resumes when overwriting unsafe



package cassdoc.operations

import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.StringUtils

import cassdoc.CommandExecServices
import cassdoc.DBCodes
import cassdoc.Detail
import cassdoc.FieldValue
import cassdoc.FixedAttr
import cassdoc.IDUtil
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelTypes
import cassdoc.commands.mutate.cassandra.MutationCmd

import com.datastax.driver.core.DataType
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken

import cwdrg.lg.annotation.Log


@Log
@CompileStatic
class CreateOperations {

  // array "stream" of docs
  // TODO: special streaming execution mode: not immediate, nor do we wait until the end.
  public static String newDocStream(CommandExecServices svcs, OperationContext opctx, Detail detail, Reader inputListOfJsonDocs, Writer outputListOfIDs)
  {
    outputListOfIDs << '['
    JsonParser parser = svcs.jsonFactory.createParser(inputListOfJsonDocs)
    JsonToken firsttoken = parser.nextToken()
    boolean firstid = true
    if (firsttoken != JsonToken.START_ARRAY) {
      throw log.err("", new Exception("newDocStream operation must begin with JSON START_ARRAY token"))
    } else {
      while (true) {
        JsonToken startObjToken = parser.nextToken()
        if (startObjToken == JsonToken.END_ARRAY) {
          outputListOfIDs << ']'
          break
        }
        else if (startObjToken != JsonToken.START_OBJECT) {
          // return list of objects created so far? no rollback....
          outputListOfIDs << ']'
          throw log.err("", new Exception("newDocStream operation, one of the array elements is not a doc"))
        } else {
          JsonToken idtoken = parser.nextToken();
          if (idtoken != JsonToken.FIELD_NAME) {
            outputListOfIDs << ']'
            throw log.err("",new Exception ("newDocStream: NO ID FIELD FOR NEW DOCUMENT"))
          } else {
            String firstField = parser.getCurrentName();
            if (!svcs.idField.equals(firstField)) {
              outputListOfIDs << ']'
              throw log.err("",new Exception ("newDocStream:ID FIELD IS NOT FIRST FIELD"))
            } else {
              String newEntityUUID = newChildDoc(svcs,opctx,detail,parser,null,null,false)
              if (firstid) {firstid = false; outputListOfIDs << '"' << newEntityUUID << '"'}
              else {outputListOfIDs << ',"' << newEntityUUID << '"'}
            }
          }
        }
      }
    }
  }


  public static String newMap(final CommandExecServices svcs, final OperationContext opctx, final Detail detail, final Map<String,Object> docMap, final boolean threaded) {

    final String docUUID = newChildDocMap(svcs,opctx,detail,docMap,null,null,threaded)
    return docUUID
  }

  public static String newDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, Reader json, boolean threaded) {
    JsonParser parser = svcs.jsonFactory.createParser(json)

    return newDoc(svcs,opctx,detail,parser, threaded)
  }


  public static String newDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, String json, boolean threaded) {

    JsonParser parser = svcs.jsonFactory.createParser(json)

    return newDoc(svcs,opctx,detail,parser,threaded)

  }

  public static String newDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser, boolean threaded) {
    JsonToken firsttoken = parser.nextToken();
    if (firsttoken == JsonToken.START_OBJECT) {

      JsonToken idtoken = parser.nextToken();
      if (idtoken != JsonToken.FIELD_NAME) {
        throw new Exception ("NO ID FIELD FOR NEW DOCUMENT");
      } else {
        String firstField = parser.getCurrentName();
        if (!svcs.idField.equals(firstField)) {
          throw new Exception("ID FIELD IS NOT FIRST FIELD");
        } else {
          String newEntityUUID = newChildDoc(svcs,opctx,detail,parser,null,null,threaded)
          return newEntityUUID
        }
      }
    } else {
      throw new Exception("Invalid Parser state")
    }

  }

  public static void newAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, String json , boolean paxos)
  {
    JsonParser parser = svcs.jsonFactory.createParser(json)
    newAttr(svcs,opctx,detail,docUUID,attr,parser,paxos)
  }

  public static void newAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, Reader json , boolean paxos)
  {
    JsonParser parser = svcs.jsonFactory.createParser(json)
    newAttr(svcs,opctx,detail,docUUID,attr,parser,paxos)
  }

  public static void newAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, JsonParser parser , boolean paxos)
  {
    FieldValue fv = parseField(svcs,opctx,detail,docUUID,attr,parser)
    MutationCmd cmd = svcs.mutations.newAttr(docUUID,attr,fv,paxos)
    analyzeNewAttrEvent(svcs,opctx,detail,docUUID,attr,fv,cmd)
  }

  // ---- parsing helper methods

  public static String newChildDoc(final CommandExecServices svcs, final OperationContext opctx, final Detail detail, final JsonParser parser, final String parentUUID, final String parentAttr, final boolean threaded) {
    final String docId = parseIDAttr(svcs,opctx,detail,parser);
    if (threaded) {
      new Thread() { public void run() { CreateOperations.performNewChildDoc(svcs,opctx,detail,parser,docId, parentUUID,parentAttr) } }.start()
    } else {
      performNewChildDoc(svcs,opctx,detail,parser,docId, parentUUID,parentAttr)
    }
    return docId
  }

  public static void performNewChildDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser, String docUUID, String parentUUID, String parentAttr)
  {
    parentUUID = StringUtils.isBlank(parentUUID) ? null : parentUUID
    // parser should be pointing at the idKey field right now
    MutationCmd newDocCmd = svcs.mutations.newDoc(docUUID,parentUUID,parentAttr)
    analyzeNewDocEvent(svcs,opctx,detail,docUUID,parentUUID,parentAttr,newDocCmd)

    while (true) {
      JsonToken nextField = parser.nextToken();
      if (nextField == JsonToken.END_OBJECT) {
        break;
      } else if (nextField == JsonToken.FIELD_NAME) {
        String fieldName = parser.getCurrentName();
        FieldValue fv = parseField(svcs,opctx,detail,docUUID,fieldName,parser);
        MutationCmd newAttrCmd = svcs.mutations.newAttr(docUUID, fieldName,fv,false)
        analyzeNewAttrEvent(svcs,opctx,detail,docUUID,fieldName,fv,newAttrCmd)
      } else {
        throw new Exception ("ILLEGAL TOKEN TYPE AT DOCUMENT ROOT "+nextField);
      }
    }

  }

  // extract or generate UUID
  public static String parseIDAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser)
  {
    JsonToken token = parser.nextToken()
    if (token == JsonToken.VALUE_STRING) {
      String idString = parser.getText();
      if (svcs.typeSvc.isKnownSuffix(idString)) {
        return IDUtil.timeUUID() + "-" + idString
      } else {
        if (svcs.typeSvc.isKnownSuffix(IDUtil.idSuffix(idString))) {
          return idString
        } else {
          throw new Exception ("Unknown type suffix for provided UUID: "+idString)
        }
      }
    }
    // TODO: more complicated stuff? DFRef object or something similar?
    throw new Exception("ID information not provided")
  }

  // for newDoc with maps rather than JSON tokens
  public static String checkIDAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String idString)
  {
    if (svcs.typeSvc.isKnownSuffix(idString)) {
      return IDUtil.timeUUID() + "-" + idString
    } else {
      if (svcs.typeSvc.isKnownSuffix(IDUtil.idSuffix(idString))) {
        return idString
      } else {
        throw new Exception ("Unknown type suffix for provided UUID: "+idString)
      }
    }
    // TODO: more complicated stuff? DFRef object or something similar?
    throw new Exception("ID information not provided")
  }


  public static FieldValue parseField(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String fieldName, JsonParser parser)
  {
    String fieldValue
    JsonToken token = parser.nextToken();

    if (token == JsonToken.VALUE_NULL) {return null}
    if (token == JsonToken.VALUE_STRING) {return new FieldValue(type:String.class,value:parser.getText())}
    if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) { return new FieldValue(type:Boolean.class,value:parser.getText())}
    if (token == JsonToken.VALUE_NUMBER_FLOAT) {return new FieldValue(type:Float.class,value:parser.getText())}
    if (token == JsonToken.VALUE_NUMBER_INT) {return new FieldValue(type:Integer.class,value:parser.getText())}

    if (token == JsonToken.START_ARRAY) {
      StringBuilder sb = new StringBuilder()
      parseIngestChildArray(svcs,opctx,detail,sb,parser,docUUID,fieldName)
      return new FieldValue(type:List.class,value:sb.toString())
    }

    if (token == JsonToken.START_OBJECT) {
      StringBuilder sb = new StringBuilder()
      parseIngestChildObject(svcs,opctx,detail,sb,parser,docUUID,fieldName)
      return new FieldValue(type:Map.class,value:sb.toString())
    }

    // else Exception
  }


  static void parseIngestChildObject(CommandExecServices svcs, OperationContext opctx, Detail detail, StringBuilder sb, JsonParser jsonParser, String parentUUID, String parentAttr)
  {
    sb << '{'
    boolean firstField = true
    String currentField = null
    while (true) {
      JsonToken token = jsonParser.nextToken()
      if (token == JsonToken.FIELD_NAME) {
        currentField = jsonParser.getCurrentName()
        if (firstField) {
          if (svcs.idField.equals(currentField)) {

            String childUUID = newChildDoc(svcs,opctx,detail,jsonParser,parentUUID,parentAttr,false)
            sb << '"' << svcs.idField << '":"' << childUUID << '"}'
            return;
          } else {
            firstField = false
          }
        } else {
          sb << ","
        }
        sb <<'"' << currentField << '":'
      }
      if (token == JsonToken.VALUE_NULL) { sb << "null" }
      if (token == JsonToken.VALUE_TRUE) { sb << "true" }
      if (token == JsonToken.VALUE_FALSE) { sb << "false" }
      if (token == JsonToken.VALUE_NUMBER_FLOAT) { sb << jsonParser.getText() }
      if (token == JsonToken.VALUE_NUMBER_INT) { sb << jsonParser.getText() }
      if (token == JsonToken.VALUE_STRING) { sb << '"' << StringEscapeUtils.escapeJson(jsonParser.getText()) << '"' }

      if (token == JsonToken.START_ARRAY) {
        // recurse
        parseIngestChildArray(svcs,opctx,detail,sb,jsonParser,parentUUID,parentAttr)
      }

      if (token == JsonToken.START_OBJECT) {
        // recurse
        parseIngestChildObject(svcs,opctx,detail, sb,jsonParser,parentUUID, parentAttr)
      }

      // unrecurse...
      if (token == JsonToken.END_OBJECT) {
        sb << '}'
        return
      }
    }
  }

  static FieldValue parseIngestChildArray(CommandExecServices svcs, OperationContext opctx, Detail detail, StringBuilder sb, JsonParser jsonParser, String parentUUID, String parentAttr)
  {
    sb << '['
    boolean firstMember = true
    int idx = 0
    while (true) {
      JsonToken token = jsonParser.nextToken()
      if (token == JsonToken.END_ARRAY) {
        sb << ']'
        // unrecurse
        return
      }

      if (firstMember) {firstMember = false} else {sb << ','}

      if (token == JsonToken.VALUE_NULL) { sb << "null" }
      if (token == JsonToken.VALUE_TRUE) { sb << "true" }
      if (token == JsonToken.VALUE_FALSE) { sb << "false" }
      if (token == JsonToken.VALUE_NUMBER_FLOAT) { sb << jsonParser.getText() }
      if (token == JsonToken.VALUE_NUMBER_INT) { sb << jsonParser.getText() }
      if (token == JsonToken.VALUE_STRING) { sb << '"' << StringEscapeUtils.escapeJson(jsonParser.getText()) << '"' }

      if (token == JsonToken.START_ARRAY) {
        // recurse
        parseIngestChildArray(svcs,opctx,detail, sb,jsonParser,parentUUID,parentAttr)
      }

      if (token == JsonToken.START_OBJECT) {
        // recurse
        parseIngestChildObject(svcs,opctx,detail, sb,jsonParser,parentUUID,parentAttr)
      }
    }
  }

  static void analyzeNewDocEvent (CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String parentUUID, String parentAttr, MutationCmd newDocCmd)
  {
    opctx.addCommand(svcs, detail, newDocCmd)

    if (parentUUID != null) {

      // TODO: "xpath"

      // parent-to-child rel
      MutationCmd newSubdocRelCmd = svcs.mutations.newRel(new Rel(p1:parentUUID,p2:parentAttr,ty1:RelTypes.TO_CHILD,c1:docUUID))
      opctx.addCommand(svcs, detail, newSubdocRelCmd)

      // child-to-parent rel
      MutationCmd newBackrefRelCmd = svcs.mutations.newRel(new Rel(p1:docUUID,ty1:RelTypes.TO_PARENT,c1:parentUUID,c2:parentAttr))
      opctx.addCommand(svcs, detail, newBackrefRelCmd)

    }

  }

  static Object convertFieldValueToFixedColValue(FieldValue attrValue, FixedAttr attrdef)
  {
    Object val = null
    switch (StringUtils.lowerCase(attrdef.coltype)) {
      case null:  // assume string/varchar/text if not specified
      case DataType.Name.ASCII.toString():
      case DataType.Name.TEXT.toString():
      case DataType.Name.VARCHAR.toString():
      case "string":
        val = attrValue?.value
        break;
      case DataType.Name.TIMESTAMP.toString():
      case "date":
      case "datetime":
        val = attrValue == null ? null : new Date(Long.parseLong(attrValue.value))
        break;
      case DataType.Name.BIGINT.toString():
      case "long":
      case "counter":
        val = attrValue == null ? null : Long.parseLong(attrValue.value)
        break;
      case DataType.Name.INT.toString():
      case "integer":
      case "int":
        val = attrValue == null ? null : Integer.parseInt(attrValue.value)
        break;
      case DataType.Name.BOOLEAN.toString():
      case "boolean":
        val = attrValue == null ? null : Boolean.parseBoolean(attrValue.value)
        break;
      case DataType.Name.FLOAT.toString():
      case "float":
        val = attrValue == null ? null : Float.parseFloat(attrValue.value)
        break;
      case DataType.Name.DOUBLE.toString():
      case "double":
        val = attrValue == null ? null : Double.parseDouble(attrValue.value)
        break;
      case DataType.Name.DECIMAL.toString():
      case "bigdecimal":
        val = attrValue == null ? null : new BigDecimal(attrValue.value)
        break;
      case DataType.Name.VARINT.toString():
      case "bigint":
      case "bigdecimal":
        val = attrValue == null ? null : new BigInteger(attrValue.value);
        break;
    }
    return val
  }

  static void analyzeNewAttrEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, FieldValue attrValue, MutationCmd cmd)
  {
    opctx.addCommand(svcs, detail, cmd)

    // fixed attr: should this be in event???
    String suffix = IDUtil.idSuffix(docUUID)
    FixedAttr attrdef = svcs.typeSvc.getTypeForSuffix(suffix).fixedAttrMap[attrName]
    String col = attrdef?.colname
    if (col != null) {
      Object val = convertFieldValueToFixedColValue(attrValue,attrdef)

      MutationCmd fixedcol = svcs.mutations.updFixedCol(docUUID,col,val)
      opctx.addCommand(svcs, detail, fixedcol)
    }


    IndexOperations.processNewAttrIndexes(svcs,opctx,detail,docUUID,attrName,attrValue)
  }

  static void addRel(final CommandExecServices svcs, final OperationContext opctx, final Detail detail, final Rel rel)
  {
    MutationCmd newRelCmd = svcs.mutations.newRel(rel)
    opctx.addCommand(svcs, detail, newRelCmd)
  }

  public static String newChildDocMap(final CommandExecServices svcs, final OperationContext opctx, final Detail detail, final Map<String,Object> docMap, final String parentUUID, final String parentAttr, final boolean threaded)
  {
    final String docUUID = checkIDAttr(svcs,opctx,detail,(String)docMap[svcs.idField])

    if (docUUID == null) throw new Exception("New document map must have id field")
    if (threaded) {
      new Thread() { public void run() { CreateOperations.performNewChildDocMap(svcs,opctx,detail,docMap.entrySet().iterator(),docUUID,parentUUID,parentAttr) } }.start()
    } else {
      CreateOperations.performNewChildDocMap(svcs,opctx,detail,docMap.entrySet().iterator(),docUUID,parentUUID,parentAttr)
    }
    return docUUID

  }

  public static String performNewChildDocMap(CommandExecServices svcs, OperationContext opctx, Detail detail, Iterator<Map.Entry<String,Object>> fields, String docUUID, String parentUUID, String parentAttr)
  {
    parentUUID = StringUtils.isBlank(parentUUID) ? null : parentUUID
    // parser should be pointing at the idKey field right now

    MutationCmd newDocCmd = svcs.mutations.newDoc(docUUID,parentUUID,parentAttr)
    analyzeNewDocEvent(svcs,opctx,detail,docUUID,parentUUID,parentAttr,newDocCmd)

    while (fields.hasNext()) {
      Map.Entry<String,Object> field = fields.next()
      if (field.key != svcs.idField) {
        FieldValue fv = serializeAttr(svcs,opctx,detail,field,docUUID,parentUUID,parentAttr)
        MutationCmd newAttrCmd = svcs.mutations.newAttr(docUUID, field.key,fv,false)
        analyzeNewAttrEvent(svcs,opctx,detail,docUUID,field.key,fv,newAttrCmd)
      }

    }

  }

  public static FieldValue serializeAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, Map.Entry<String,Object> field, String docId, String parentUUID, String parentAttr)
  {
    FieldValue fv = new FieldValue()

    if (field.value == null) {
      fv.value = null
      fv.type = null
    } else if (field.value instanceof CharSequence) {
      fv.value = field.value.toString()
      fv.type = DBCodes.TYPE_CODE_STRING
    } else if (field.value instanceof float || field.value instanceof double || field.value instanceof BigDecimal) {
      fv.value = field.value.toString()
      fv.type = DBCodes.TYPE_CODE_DECIMAL
    } else if (field.value instanceof byte || field.value instanceof int || field.value instanceof long || field.value instanceof BigInteger) {
      fv.value = field.value.toString()
      fv.type = DBCodes.TYPE_CODE_INTEGER
    } else if (field.value instanceof List) {
      fv.value = serializeList(svcs,opctx,detail,docId,field.key,(List)field.value)
      fv.type = DBCodes.TYPE_CODE_ARRAY
    } else if (field.value instanceof Map) {
      fv.value = serializeMap(svcs,opctx,detail,docId,field.key,(Map)field.value)
      fv.type = DBCodes.TYPE_CODE_OBJECT
    }

    return fv

  }

  public static String serializeList(CommandExecServices svcs, OperationContext opctx, Detail detail, String docId, String curAttr, List list)
  {
    StringWriter sw = new StringWriter()
    sw << "["
    boolean first = true
    for (Object o : list) {
      if (first) first = false; else sw << ",";
      if (o == null)
        sw << "null"
      if (o instanceof CharSequence || o instanceof float || o instanceof double || o instanceof BigDecimal || o instanceof int || o instanceof byte || o instanceof long || o instanceof BigDecimal) {
        sw << '"' << StringEscapeUtils.escapeJson(o.toString()) << '"'
      } else if (o instanceof List) {
        sw << serializeList(svcs,opctx,detail,docId,curAttr,(List)o)
      } else if (o instanceof Map) {
        sw << serializeMap(svcs,opctx,detail,docId,curAttr,(Map)o)
      }
    }
    sw << "]"
    return sw.toString()
  }

  public static String serializeMap(CommandExecServices svcs, OperationContext opctx, Detail detail, String docId, String curAttr, Map map)
  {
    StringWriter sw = new StringWriter()
    // determine if this is a new child document, or just a map value
    if (map.containsKey(svcs.idField)) {
      final String childDocUUID = newChildDocMap(svcs,opctx,detail,map,docId,curAttr,false)
      sw << '{"_id":"' << childDocUUID << '"}'
    } else {
      Iterator<Map.Entry<String,Object>> fields = map.entrySet().iterator()
      boolean first = true
      while (fields.hasNext()) {
        Map.Entry<String,Object> field = fields.next()
        if (first) first = false; else sw << ","
        sw << '"' <<StringEscapeUtils.escapeJson(field.key) << '":'
        Object o = field.value
        if (o == null)
          sw << "null"
        if (o instanceof CharSequence || o instanceof float || o instanceof double || o instanceof BigDecimal || o instanceof int || o instanceof byte || o instanceof long || o instanceof BigDecimal) {
          sw << '"' << StringEscapeUtils.escapeJson(o.toString()) << '"'
        } else if (o instanceof List) {
          sw << serializeList(svcs,opctx,detail,docId,curAttr,(List)o)
        } else if (o instanceof Map) {
          sw << serializeMap(svcs,opctx,detail,docId,curAttr,(Map)o)
        }
      }
    }
  }


}

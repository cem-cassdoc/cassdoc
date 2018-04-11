package cassdoc.operations

import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.StringUtils

import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.FieldValue
import cassdoc.IDUtil
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelTypes
import cassdoc.commands.mutate.cassandra.MutationCmd
import cassdoc.commands.retrieve.RPUtil
import cassdoc.commands.retrieve.RowProcessor

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken

@CompileStatic
class UpdateOperations {

  static void updateAttrPAXOS(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, String json, UUID checkVal) {
    JsonParser parser = svcs.jsonFactory.createParser(json)
    FieldValue fv = CreateOperations.parseField(svcs,opctx,detail,docUUID,attr,parser)
    MutationCmd cmd = svcs.mutations.updAttrPAXOS(docUUID, attr, fv, checkVal,["P", docUUID])
    analyzeUpdateAttrEvent(svcs,opctx,detail,docUUID,attr,fv,cmd)
  }

  static void updateAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, String json) {
    JsonParser parser = svcs.jsonFactory.createParser(json)
    FieldValue fv = CreateOperations.parseField(svcs,opctx,detail,docUUID,attr,parser)
    MutationCmd cmd = svcs.mutations.updAttr(docUUID,attr,fv)
    analyzeUpdateAttrEvent(svcs,opctx,detail,docUUID,attr,fv,cmd)
  }

  static void updateAttrEntry(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, Map.Entry<String,Object> attr) {
    FieldValue fv =  CreateOperations.serializeAttr(svcs,opctx,detail,attr,docUUID,null,null)
    MutationCmd cmd = svcs.mutations.updAttr(docUUID, attr.key,fv)
    analyzeUpdateAttrEvent(svcs,opctx,detail,docUUID,attr.key,fv,cmd)
  }

  static void analyzeUpdateAttrEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, FieldValue attrValue, MutationCmd cmd) {
    // PAXOS updates: prepare the clears and new-writes, but the other writes must only be executed if the initial PAXOS update succeeds
    // ...probably will occur in optimization  and execution...

    RowProcessor relCmd = svcs.retrievals.getAttrRelsRP(docUUID,[
      RelTypes.SYS_INDEX,
      RelTypes.TO_CHILD
    ],attrName)
    relCmd.initiateQuery(svcs, opctx, detail, null)
    List<Rel> rels = RPUtil.getAllRels(relCmd)

    // cleanup (TODO: separate thread?)
    DeleteOperations.analyzeDeleteAttrEvent(svcs, opctx, detail, docUUID,attrName, rels, true)
    // write new value
    CreateOperations.analyzeNewAttrEvent(svcs, opctx, detail,docUUID,attrName,attrValue,cmd)
  }


  // ---- begin OVERLAY code: it's complicated


  // overlay attribute JSON will assume fully-fleshed ids are extant docs, will create new doc refs, and will clean up the rels
  // If any child object has any content besides the reference _id, it will be ignored and discarded.
  // Overlay only processes the attribute document content, NEW docs, and EXTANT doc references
  // We do not validate "extant" ids as already existing. Um, nor is there security checking on it yet
  // ... for cupcake compatibility... this should handle most of APIs that modify part of a cupcake property
  // TODO: strict mode where Ignore coroutines throw exceptions if anytinhg except bare IDrefs is present in extant subdocs
  // TODO: paxos version... shouldn't be too hard
  static Set<String> updateAttrOverlay(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, String json)
  {
    JsonParser parser = svcs.jsonFactory.createParser(json)
    AttrOverlayTracking overlayTracker = new AttrOverlayTracking()
    FieldValue fv = parseFieldOverlay(svcs,opctx,detail,docUUID,attr,parser,overlayTracker)
    MutationCmd cmd = svcs.mutations.updAttr(docUUID, attr, fv)
    analyzeUpdateOverlayAttrEvent(svcs, opctx, detail, docUUID, attr, fv, cmd)
    return overlayTracker.newIDs
  }

  static void analyzeUpdateOverlayAttrEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, FieldValue attrValue, MutationCmd cmd) {

    RowProcessor relCmd = svcs.retrievals.getAttrRelsRP(docUUID,[
      RelTypes.SYS_INDEX,
      RelTypes.TO_CHILD]
    ,attrName)
    relCmd.initiateQuery(svcs, opctx, detail, null)
    List<Rel> rels = RPUtil.getAllRels(relCmd)

    // cleanup (TODO: separate thread?)
    DeleteOperations.analyzeDeleteAttrEvent(svcs, opctx, detail, docUUID, attrName, rels, true)
    // write new value
    CreateOperations.analyzeNewAttrEvent(svcs, opctx, detail,docUUID,attrName,attrValue,cmd)
  }


  public static FieldValue parseFieldOverlay(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String fieldName, JsonParser parser, AttrOverlayTracking overlayTracker)
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
      parseOverlayChildArray(svcs,opctx,detail,sb,parser,docUUID,fieldName, overlayTracker)
      return new FieldValue(type:List.class,value:sb.toString())
    }

    if (token == JsonToken.START_OBJECT) {
      StringBuilder sb = new StringBuilder()
      parseOverlayChildObject(svcs,opctx,detail,sb,parser,docUUID,fieldName, overlayTracker)
      return new FieldValue(type:Map.class,value:sb.toString())
    }

    // else Exception
  }

  // extract or generate UUID
  public static String[] parseIDAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser)
  {
    JsonToken token = parser.nextToken()
    if (token == JsonToken.VALUE_STRING) {
      String idString = parser.getText();
      if (svcs.typeSvc.isKnownSuffix(idString)) {
        return [
          "NEW",
          IDUtil.timeUUID() + "-" + idString] as String[]
      } else {
        if (svcs.typeSvc.isKnownSuffix(IDUtil.idSuffix(idString))) {
          return ["EXTANT", idString] as String[]
        } else {
          throw new Exception ("Unknwon type suffix for provided UUID "+idString)
        }
      }
    }
    // TODO: more complicated stuff? DFRef object or something similar?
    throw new Exception("ID information not provided")
  }


  public static String overlayChildDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser, String parentUUID, String parentAttr, AttrOverlayTracking overlayTracker) {


    String[] analyzeID = parseIDAttr(svcs,opctx,detail,parser);
    if (analyzeID[0] == "NEW") {
      overlayTracker.newIDs.add(analyzeID[1])
      // parser should be pointing at the idKey field right no
      parentUUID = StringUtils.isBlank(parentUUID) ? null : parentUUID
      String docUUID = analyzeID[1]
      MutationCmd newDocCmd = svcs.mutations.newDoc(docUUID, parentUUID, parentAttr)
      CreateOperations.analyzeNewDocEvent(svcs,opctx,detail,docUUID,parentUUID,parentAttr,newDocCmd)

      while (true) {
        JsonToken nextField = parser.nextToken();
        if (nextField == JsonToken.END_OBJECT) {
          break;
        } else if (nextField == JsonToken.FIELD_NAME) {
          String fieldName = parser.getCurrentName();
          FieldValue fv = parseFieldOverlay(svcs,opctx,detail,docUUID,fieldName,parser, overlayTracker);
          MutationCmd newAttrCmd = svcs.mutations.newAttr(docUUID, fieldName,fv,false)
          CreateOperations.analyzeNewAttrEvent(svcs,opctx,detail,docUUID,fieldName,fv,newAttrCmd)
        } else {
          throw new Exception ("ILLEGAL TOKEN TYPE AT DOCUMENT ROOT "+nextField);
        }
      }
      return docUUID
    } else {
      overlayTracker.extantIDs.add(analyzeID[1])

      // parent-to-child rel
      MutationCmd newSubdocRelCmd = svcs.mutations.newRel(new Rel(p1:parentUUID,ty1:RelTypes.TO_CHILD,p2:parentAttr,c1:analyzeID[1]))
      opctx.addCommand(svcs, detail, newSubdocRelCmd)

      // child-to-parent rel
      MutationCmd newBackrefRelCmd = svcs.mutations.newRel(new Rel(p1:analyzeID[1],ty1:RelTypes.TO_PARENT,c1:parentUUID,c2:parentAttr))
      opctx.addCommand(svcs, detail, newBackrefRelCmd)

      // extant doc... overlay does not modify extant docs, so we ignore anything inside of it
      while (true) {
        JsonToken nextField = parser.nextToken();
        String fieldName = parser.getCurrentName();
        if (nextField == JsonToken.END_OBJECT) {
          break;
        } else if (nextField == JsonToken.FIELD_NAME) {
          parseFieldIgnore(svcs,opctx,detail,analyzeID[1],fieldName,parser);
        } else {
          throw new Exception ("ILLEGAL TOKEN TYPE AT DOCUMENT ROOT "+nextField);
        }
      }
      return analyzeID[1]
    }
  }


  static void parseOverlayChildObject(CommandExecServices svcs, OperationContext opctx, Detail detail, StringBuilder sb, JsonParser jsonParser, String parentUUID, String parentAttr, AttrOverlayTracking overlayTracker)
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

            String childUUID = overlayChildDoc(svcs,opctx,detail,jsonParser,parentUUID,parentAttr, overlayTracker)
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
        parseOverlayChildArray(svcs,opctx,detail,sb,jsonParser,parentUUID,parentAttr, overlayTracker)
      }

      if (token == JsonToken.START_OBJECT) {
        // recurse
        parseOverlayChildObject(svcs,opctx,detail, sb,jsonParser,parentUUID, parentAttr, overlayTracker)
      }

      // unrecurse...
      if (token == JsonToken.END_OBJECT) {
        sb << '}'
        return
      }
    }
  }

  static FieldValue parseOverlayChildArray(CommandExecServices svcs, OperationContext opctx, Detail detail, StringBuilder sb, JsonParser jsonParser, String parentUUID, String parentAttr, AttrOverlayTracking overlayTracker)
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
        parseOverlayChildArray(svcs,opctx,detail, sb,jsonParser,parentUUID,parentAttr, overlayTracker)
      }

      if (token == JsonToken.START_OBJECT) {
        // recurse
        parseOverlayChildObject(svcs,opctx,detail, sb,jsonParser,parentUUID,parentAttr, overlayTracker)
      }
    }
  }

  /**
   * While parsing a stream token, if behavior says we need to ignore fields, this and the Child / Array associated 
   * routines eat the tokens, ignoring them until the parent field, array, or object ends. 
   * 
   * @param svcs
   * @param opctx
   * @param detail
   * @param docUUID
   * @param fieldName
   * @param parser
   */
  static void parseFieldIgnore(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String fieldName, JsonParser parser)
  {
    String fieldValue
    JsonToken token = parser.nextToken();

    if (token == JsonToken.VALUE_NULL) { return }
    if (token == JsonToken.VALUE_STRING) { return }
    if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) { return }
    if (token == JsonToken.VALUE_NUMBER_FLOAT) { return }
    if (token == JsonToken.VALUE_NUMBER_INT) { return }

    if (token == JsonToken.START_ARRAY) {
      parseIgnoreChildArray(svcs,opctx,detail,parser,docUUID,fieldName)
      return
    }

    if (token == JsonToken.START_OBJECT) {
      parseIgnoreChildObject(svcs,opctx,detail,parser,docUUID,fieldName)
      return
    }

    // else Exception
  }

  static void parseIgnoreChildObject(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser jsonParser, String parentUUID, String parentAttr)
  {
    while (true) {
      JsonToken token = jsonParser.nextToken()

      if (token == JsonToken.START_ARRAY) {
        // recurse (ignore)
        parseIgnoreChildArray(svcs,opctx,detail,jsonParser,parentUUID,parentAttr)
      }

      if (token == JsonToken.START_OBJECT) {
        // recurse (ignore)
        parseIgnoreChildObject(svcs,opctx,detail, jsonParser,parentUUID, parentAttr)
      }

      // unrecurse...
      if (token == JsonToken.END_OBJECT) { return }
    }
  }

  static FieldValue parseIgnoreChildArray(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser jsonParser, String parentUUID, String parentAttr)
  {
    while (true) {
      JsonToken token = jsonParser.nextToken()
      if (token == JsonToken.END_ARRAY) {
        // unrecurse
        return
      }

      if (token == JsonToken.START_ARRAY) {
        // recurse
        parseIgnoreChildArray(svcs,opctx,detail, jsonParser,parentUUID,parentAttr)
      }

      if (token == JsonToken.START_OBJECT) {
        // recurse
        parseIgnoreChildObject(svcs,opctx,detail, jsonParser,parentUUID,parentAttr)
      }
    }
  }

  // ---- END OVERLAY code
}

@CompileStatic
class AttrOverlayTracking
{
  Set<String> extantIDs = [] as Set
  Set<String> newIDs = [] as Set
}


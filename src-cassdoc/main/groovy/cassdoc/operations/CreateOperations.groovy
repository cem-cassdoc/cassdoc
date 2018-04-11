package cassdoc.operations

import cassdoc.*
import cassdoc.commands.mutate.NewAttr
import cassdoc.commands.mutate.NewDoc
import cassdoc.commands.mutate.NewRel
import cassdoc.commands.mutate.UpdFixedCol
import com.datastax.driver.core.DataType
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import cwdrg.lg.annotation.Log
import groovy.transform.CompileStatic
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.commons.lang3.StringUtils

@Log
@CompileStatic
class CreateOperations {

    // array "stream" of docs
    // TODO: special streaming execution mode: not immediate, nor do we wait until the end.
    static String newDocStream(CommandExecServices svcs, OperationContext opctx, Detail detail, Reader inputListOfJsonDocs, Writer outputListOfIDs) {
        outputListOfIDs << '['
        JsonParser parser = svcs.jsonFactory.createParser(inputListOfJsonDocs)
        JsonToken firsttoken = parser.nextToken()
        boolean firstid = true
        if (firsttoken != JsonToken.START_ARRAY) {
            throw log.err("", new IllegalArgumentException("newDocStream operation must begin with JSON START_ARRAY token"))
        } else {
            while (true) {
                JsonToken startObjToken = parser.nextToken()
                if (startObjToken == JsonToken.END_ARRAY) {
                    outputListOfIDs << ']'
                    break
                } else if (startObjToken != JsonToken.START_OBJECT) {
                    // return list of objects created so far? no rollback....
                    outputListOfIDs << ']'
                    throw log.err("", new IllegalArgumentException("newDocStream operation, one of the array elements is not a doc"))
                } else {
                    JsonToken idtoken = parser.nextToken()
                    if (idtoken != JsonToken.FIELD_NAME) {
                        outputListOfIDs << ']'
                        throw log.err("", new IllegalArgumentException("newDocStream: NO ID FIELD FOR NEW DOCUMENT"))
                    } else {
                        String firstField = parser.getCurrentName()
                        if (!svcs.idField.equals(firstField)) {
                            outputListOfIDs << ']'
                            throw log.err("", new IllegalArgumentException("newDocStream:ID FIELD IS NOT FIRST FIELD"))
                        } else {
                            String newEntityUUID = newChildDoc(svcs, opctx, detail, parser, null, null, false)
                            if (firstid) {
                                firstid = false
                                outputListOfIDs << '"' << newEntityUUID << '"'
                            } else {
                                outputListOfIDs << ',"' << newEntityUUID << '"'
                            }
                        }
                    }
                }
            }
        }
    }

    static String newMap(
            final CommandExecServices svcs,
            final OperationContext opctx,
            final Detail detail, final Map<String, Object> docMap, final boolean threaded) {

        final String docUUID = newChildDocMap(svcs, opctx, detail, docMap, null, null, threaded)
        return docUUID
    }

    static String newDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, Reader json, boolean threaded) {
        JsonParser parser = svcs.jsonFactory.createParser(json)

        return newDoc(svcs, opctx, detail, parser, threaded)
    }

    static String newDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, String json, boolean threaded) {

        JsonParser parser = svcs.jsonFactory.createParser(json)

        return newDoc(svcs, opctx, detail, parser, threaded)

    }

    static String newDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser, boolean threaded) {
        JsonToken firsttoken = parser.nextToken()
        if (firsttoken == JsonToken.START_OBJECT) {

            JsonToken idtoken = parser.nextToken()
            if (idtoken != JsonToken.FIELD_NAME) {
                throw new IllegalArgumentException("NO ID FIELD FOR NEW DOCUMENT")
            } else {
                String firstField = parser.getCurrentName()
                if (!svcs.idField.equals(firstField)) {
                    throw new IllegalArgumentException("ID FIELD IS NOT FIRST FIELD")
                } else {
                    String newEntityUUID = newChildDoc(svcs, opctx, detail, parser, null, null, threaded)
                    return newEntityUUID
                }
            }
        } else {
            throw new IllegalArgumentException("Invalid Parser state")
        }
    }

    static void newAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, String json, boolean paxos) {
        JsonParser parser = svcs.jsonFactory.createParser(json)
        newAttr(svcs, opctx, detail, docUUID, attr, parser, paxos)
    }

    static void newAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, Reader json, boolean paxos) {
        JsonParser parser = svcs.jsonFactory.createParser(json)
        newAttr(svcs, opctx, detail, docUUID, attr, parser, paxos)
    }

    static void newAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, JsonParser parser, boolean paxos) {
        NewAttr cmd = new NewAttr(docUUID: docUUID, attrName: attr)
        cmd.attrValue = parseField(svcs, opctx, detail, docUUID, attr, parser)
        cmd.isComplete = true
        cmd.paxos = paxos
        analyzeNewAttrEvent(svcs, opctx, detail, cmd)
    }

    // ---- parsing helper methods

    static String newChildDoc(
            final CommandExecServices svcs,
            final OperationContext opctx,
            final Detail detail,
            final JsonParser parser, final String parentUUID, final String parentAttr, final boolean threaded) {
        final String docId = parseIDAttr(svcs, opctx, detail, parser)
        if (threaded) {
            new Thread() {
                void run() {
                    CreateOperations.performNewChildDoc(svcs, opctx, detail, parser, docId, parentUUID, parentAttr)
                }
            }.start()
        } else {
            performNewChildDoc(svcs, opctx, detail, parser, docId, parentUUID, parentAttr)
        }
        return docId
    }

    static void performNewChildDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser, String docId, String parentUUID, String parentAttr) {
        // parser should be pointing at the idKey field right now
        NewDoc newDocCmd = new NewDoc()
        newDocCmd.docUUID = docId
        newDocCmd.parentUUID = StringUtils.isBlank(parentUUID) ? null : parentUUID
        newDocCmd.parentAttr = parentAttr
        newDocCmd.isComplete = true

        analyzeNewDocEvent(svcs, opctx, detail, newDocCmd)

        while (true) {
            JsonToken nextField = parser.nextToken()
            if (nextField == JsonToken.END_OBJECT) {
                break
            } else if (nextField == JsonToken.FIELD_NAME) {
                String fieldName = parser.getCurrentName()
                NewAttr newAttrCmd = new NewAttr(docUUID: newDocCmd.docUUID, attrName: fieldName)
                newAttrCmd.attrValue = parseField(svcs, opctx, detail, newDocCmd.docUUID, fieldName, parser)
                newAttrCmd.isComplete = true
                analyzeNewAttrEvent(svcs, opctx, detail, newAttrCmd)
            } else {
                throw new IllegalArgumentException("ILLEGAL TOKEN TYPE AT DOCUMENT ROOT " + nextField)
            }
        }

    }

    // extract or generate UUID

    static String parseIDAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, JsonParser parser) {
        JsonToken token = parser.nextToken()
        if (token == JsonToken.VALUE_STRING) {
            String idString = parser.getText()
            if (svcs.collections[opctx.space].first.isKnownSuffix(idString)) {
                return IDUtil.timeUUID() + "-" + idString
            } else {
                if (svcs.collections[opctx.space].first.isKnownSuffix(IDUtil.idSuffix(idString))) {
                    return idString
                } else {
                    throw new IllegalArgumentException("Unknown type suffix for provided UUID: " + idString)
                }
            }
        }
        // TODO: more complicated stuff? DFRef object or something similar?
        throw new IllegalArgumentException("ID information not provided")
    }

    // for newDoc with maps rather than JSON tokens
    static String checkIDAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String idString) {
        if (svcs.collections[opctx.space].first.isKnownSuffix(idString)) {
            return IDUtil.timeUUID() + "-" + idString
        } else {
            if (svcs.collections[opctx.space].first.isKnownSuffix(IDUtil.idSuffix(idString))) {
                return idString
            } else {
                throw new IllegalArgumentException("Unknown type suffix for provided UUID: " + idString)
            }
        }
        // TODO: more complicated stuff? DFRef object or something similar?
        throw new IllegalArgumentException("ID information not provided")
    }

    static FieldValue parseField(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String fieldName, JsonParser parser) {
        String fieldValue
        JsonToken token = parser.nextToken()

        if (token == JsonToken.VALUE_NULL) {
            return null
        }
        if (token == JsonToken.VALUE_STRING) {
            return new FieldValue(type: String, value: parser.getText())
        }
        if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
            return new FieldValue(type: Boolean, value: parser.getText())
        }
        if (token == JsonToken.VALUE_NUMBER_FLOAT) {
            return new FieldValue(type: Float, value: parser.getText())
        }
        if (token == JsonToken.VALUE_NUMBER_INT) {
            return new FieldValue(type: Integer, value: parser.getText())
        }

        if (token == JsonToken.START_ARRAY) {
            StringBuilder sb = new StringBuilder()
            parseIngestChildArray(svcs, opctx, detail, sb, parser, docUUID, fieldName)
            return new FieldValue(type: List, value: sb.toString())
        }

        if (token == JsonToken.START_OBJECT) {
            StringBuilder sb = new StringBuilder()
            parseIngestChildObject(svcs, opctx, detail, sb, parser, docUUID, fieldName)
            return new FieldValue(type: Map, value: sb.toString())
        }

        // else Exception
    }

    static void parseIngestChildObject(CommandExecServices svcs, OperationContext opctx, Detail detail, StringBuilder sb, JsonParser jsonParser, String parentUUID, String parentAttr) {
        sb << '{'
        boolean firstField = true
        String currentField = null
        while (true) {
            JsonToken token = jsonParser.nextToken()
            if (token == JsonToken.FIELD_NAME) {
                currentField = jsonParser.getCurrentName()
                if (firstField) {
                    if (svcs.idField.equals(currentField)) {

                        String childUUID = newChildDoc(svcs, opctx, detail, jsonParser, parentUUID, parentAttr, false)
                        sb << '"' << svcs.idField << '":"' << childUUID << '"}'
                        return
                    } else {
                        firstField = false
                    }
                } else {
                    sb << ","
                }
                sb << '"' << currentField << '":'
            }
            if (token == JsonToken.VALUE_NULL) {
                sb << "null"
            }
            if (token == JsonToken.VALUE_TRUE) {
                sb << "true"
            }
            if (token == JsonToken.VALUE_FALSE) {
                sb << "false"
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                sb << jsonParser.getText()
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                sb << jsonParser.getText()
            }
            if (token == JsonToken.VALUE_STRING) {
                sb << '"' << StringEscapeUtils.escapeJson(jsonParser.getText()) << '"'
            }

            if (token == JsonToken.START_ARRAY) {
                // recurse
                parseIngestChildArray(svcs, opctx, detail, sb, jsonParser, parentUUID, parentAttr)
            }

            if (token == JsonToken.START_OBJECT) {
                // recurse
                parseIngestChildObject(svcs, opctx, detail, sb, jsonParser, parentUUID, parentAttr)
            }

            // unrecurse...
            if (token == JsonToken.END_OBJECT) {
                sb << '}'
                return
            }
        }
    }

    static FieldValue parseIngestChildArray(CommandExecServices svcs, OperationContext opctx, Detail detail, StringBuilder sb, JsonParser jsonParser, String parentUUID, String parentAttr) {
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

            if (firstMember) {
                firstMember = false
            } else {
                sb << ','
            }

            if (token == JsonToken.VALUE_NULL) {
                sb << "null"
            }
            if (token == JsonToken.VALUE_TRUE) {
                sb << "true"
            }
            if (token == JsonToken.VALUE_FALSE) {
                sb << "false"
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                sb << jsonParser.getText()
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                sb << jsonParser.getText()
            }
            if (token == JsonToken.VALUE_STRING) {
                sb << '"' << StringEscapeUtils.escapeJson(jsonParser.getText()) << '"'
            }

            if (token == JsonToken.START_ARRAY) {
                // recurse
                parseIngestChildArray(svcs, opctx, detail, sb, jsonParser, parentUUID, parentAttr)
            }

            if (token == JsonToken.START_OBJECT) {
                // recurse
                parseIngestChildObject(svcs, opctx, detail, sb, jsonParser, parentUUID, parentAttr)
            }
        }
    }

    static void analyzeNewDocEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, NewDoc newDocCmd) {
        opctx.addCommand(svcs, detail, newDocCmd)

        if (newDocCmd.parentUUID != null) {

            // TODO: "xpath"

            // parent-to-child rel
            NewRel newSubdocRelCmd = new NewRel()
            newSubdocRelCmd.p1 = newDocCmd.parentUUID
            newSubdocRelCmd.ty1 = "CH"
            newSubdocRelCmd.p2 = newDocCmd.parentAttr
            newSubdocRelCmd.c1 = newDocCmd.docUUID
            opctx.addCommand(svcs, detail, newSubdocRelCmd)

            // child-to-parent rel
            NewRel newBackrefRelCmd = new NewRel()
            newBackrefRelCmd.p1 = newDocCmd.docUUID
            newBackrefRelCmd.ty1 = "-CH"
            newBackrefRelCmd.c1 = newDocCmd.parentUUID
            newBackrefRelCmd.c2 = newDocCmd.parentAttr
            opctx.addCommand(svcs, detail, newBackrefRelCmd)

        }

    }

    static void analyzeNewAttrEvent(CommandExecServices svcs, OperationContext opctx, Detail detail, NewAttr cmd) {
        opctx.addCommand(svcs, detail, cmd)

        // fixed attr: should this be in event???
        String suffix = IDUtil.idSuffix(cmd.docUUID)
        FixedAttr attrdef = svcs.collections[opctx.space].first.getTypeForSuffix(suffix).fixedAttrMap[cmd.attrName]
        String col = attrdef?.colname
        if (col != null) {
            Object val = null
            switch (StringUtils.lowerCase(attrdef.coltype)) {
                case null:  // assume string/varchar/text if not specified
                case DataType.Name.ASCII.toString():
                case DataType.Name.TEXT.toString():
                case DataType.Name.VARCHAR.toString():
                case "string":
                    val = cmd.attrValue?.value
                    break
                case DataType.Name.TIMESTAMP.toString():
                case "date":
                case "datetime":
                    val = cmd.attrValue == null ? null : new Date(Long.parseLong(cmd.attrValue.value))
                    break
                case DataType.Name.BIGINT.toString():
                case "long":
                case "counter":
                    val = cmd.attrValue == null ? null : Long.parseLong(cmd.attrValue.value)
                    break
                case DataType.Name.INT.toString():
                case "integer":
                case "int":
                    val = cmd.attrValue == null ? null : Integer.parseInt(cmd.attrValue.value)
                    break
                case DataType.Name.BOOLEAN.toString():
                case "boolean":
                    val = cmd.attrValue == null ? null : Boolean.parseBoolean(cmd.attrValue.value)
                    break
                case DataType.Name.FLOAT.toString():
                case "float":
                    val = cmd.attrValue == null ? null : Float.parseFloat(cmd.attrValue.value)
                    break
                case DataType.Name.DOUBLE.toString():
                case "double":
                    val = cmd.attrValue == null ? null : Double.parseDouble(cmd.attrValue.value)
                    break
                case DataType.Name.DECIMAL.toString():
                case "bigdecimal":
                    val = cmd.attrValue == null ? null : new BigDecimal(cmd.attrValue.value)
                    break
                case DataType.Name.VARINT.toString():
                case "bigint":
                    val = cmd.attrValue == null ? null : new BigInteger(cmd.attrValue.value)
                    break
            }
            UpdFixedCol fixedcol = new UpdFixedCol(docUUID: cmd.docUUID, colName: col, value: val)
            opctx.addCommand(svcs, detail, fixedcol)
        }


        IndexOperations.processNewAttrIndexes(svcs, opctx, detail, cmd)
    }

    static void addRel(
            final CommandExecServices svcs, final OperationContext opctx, final Detail detail, final Rel rel) {
        NewRel newRelCmd = new NewRel()
        newRelCmd.p1 = rel.p1
        newRelCmd.ty1 = rel.ty1
        newRelCmd.ty2 = rel.ty2
        newRelCmd.ty3 = rel.ty3
        newRelCmd.ty4 = rel.ty4
        newRelCmd.p2 = rel.p2
        newRelCmd.p3 = rel.p3
        newRelCmd.p4 = rel.p4
        newRelCmd.c1 = rel.c1
        newRelCmd.c2 = rel.c2
        newRelCmd.c3 = rel.c3
        newRelCmd.c4 = rel.c4
        newRelCmd.link = rel.lk
        newRelCmd.d = rel.d
        // metadata isn't allowed. Must use other metadata APIs for that. to avoid the user making their own ids
        newRelCmd.execMutationCassandra(svcs, opctx, detail)
    }

    static String newChildDocMap(
            final CommandExecServices svcs,
            final OperationContext opctx,
            final Detail detail,
            final Map<String, Object> docMap,
            final String parentUUID, final String parentAttr, final boolean threaded) {
        final String docUUID = checkIDAttr(svcs, opctx, detail, (String) docMap[svcs.idField])

        if (docUUID == null) throw new IllegalArgumentException("New document map must have id field")
        if (threaded) {
            new Thread() {
                void run() {
                    CreateOperations.performNewChildDocMap(svcs, opctx, detail, docMap.entrySet().iterator(), docUUID, parentUUID, parentAttr)
                }
            }.start()
        } else {
            CreateOperations.performNewChildDocMap(svcs, opctx, detail, docMap.entrySet().iterator(), docUUID, parentUUID, parentAttr)
        }
        return docUUID

    }


    static String performNewChildDocMap(CommandExecServices svcs, OperationContext opctx, Detail detail, Iterator<Map.Entry<String, Object>> fields, String docId, String parentUUID, String parentAttr) {
        // parser should be pointing at the idKey field right now
        NewDoc newDocCmd = new NewDoc()
        newDocCmd.docUUID = docId
        newDocCmd.parentUUID = StringUtils.isBlank(parentUUID) ? null : parentUUID
        newDocCmd.parentAttr = parentAttr
        newDocCmd.isComplete = true

        analyzeNewDocEvent(svcs, opctx, detail, newDocCmd)

        while (fields.hasNext()) {
            Map.Entry<String, Object> field = fields.next()
            if (field.key != svcs.idField) {
                FieldValue fv = serializeAttr(svcs, opctx, detail, field, docId, parentUUID, parentAttr)
                NewAttr newAttrCmd = new NewAttr(docUUID: docId, attrName: field.key)
                newAttrCmd.attrValue = fv
                newAttrCmd.isComplete = true
                analyzeNewAttrEvent(svcs, opctx, detail, newAttrCmd)
            }

        }

    }


    static FieldValue serializeAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, Map.Entry<String, Object> field, String docId, String parentUUID, String parentAttr) {
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
            fv.value = serializeList(svcs, opctx, detail, docId, field.key, (List) field.value)
            fv.type = DBCodes.TYPE_CODE_ARRAY
        } else if (field.value instanceof Map) {
            fv.value = serializeMap(svcs, opctx, detail, docId, field.key, (Map) field.value)
            fv.type = DBCodes.TYPE_CODE_OBJECT
        }

        return fv

    }

    static String serializeList(CommandExecServices svcs, OperationContext opctx, Detail detail, String docId, String curAttr, List list) {
        StringWriter sw = new StringWriter()
        sw << "["
        boolean first = true
        for (Object o : list) {
            if (first) first = false else sw << ","
            if (o == null)
                sw << "null"
            if (o instanceof CharSequence || o instanceof float || o instanceof double || o instanceof BigDecimal || o instanceof int || o instanceof byte || o instanceof long || o instanceof BigDecimal) {
                sw << '"' << StringEscapeUtils.escapeJson(o.toString()) << '"'
            } else if (o instanceof List) {
                sw << serializeList(svcs, opctx, detail, docId, curAttr, (List) o)
            } else if (o instanceof Map) {
                sw << serializeMap(svcs, opctx, detail, docId, curAttr, (Map) o)
            }
        }
        sw << "]"
        return sw.toString()
    }

    static String serializeMap(CommandExecServices svcs, OperationContext opctx, Detail detail, String docId, String curAttr, Map map) {
        StringWriter sw = new StringWriter()
        // determine if this is a new child document, or just a map value
        if (map.containsKey(svcs.idField)) {
            final String childDocUUID = newChildDocMap(svcs, opctx, detail, map, docId, curAttr, false)
            sw << '{"_id":"' << childDocUUID << '"}'
        } else {
            Iterator<Map.Entry<String, Object>> fields = map.entrySet().iterator()
            boolean first = true
            while (fields.hasNext()) {
                Map.Entry<String, Object> field = fields.next()
                if (first) first = false else sw << ","
                sw << '"' << StringEscapeUtils.escapeJson(field.key) << '":'
                Object o = field.value
                if (o == null)
                    sw << "null"
                if (o instanceof CharSequence || o instanceof float || o instanceof double || o instanceof BigDecimal || o instanceof int || o instanceof byte || o instanceof long || o instanceof BigDecimal) {
                    sw << '"' << StringEscapeUtils.escapeJson(o.toString()) << '"'
                } else if (o instanceof List) {
                    sw << serializeList(svcs, opctx, detail, docId, curAttr, (List) o)
                } else if (o instanceof Map) {
                    sw << serializeMap(svcs, opctx, detail, docId, curAttr, (Map) o)
                }
            }
        }
    }

}

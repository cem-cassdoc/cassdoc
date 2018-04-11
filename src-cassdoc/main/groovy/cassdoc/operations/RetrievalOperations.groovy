package cassdoc.operations

import cassdoc.commands.retrieve.QueryToListOfStrArr
import cassdoc.exceptions.InvalidTypeException
import cassdoc.exceptions.ResourceNotFoundException
import groovy.transform.CompileStatic

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

import org.apache.commons.lang3.StringEscapeUtils

import cassdoc.AttrNames
import cassdoc.CommandExecServices
import cassdoc.DBCodes
import cassdoc.Detail
import cassdoc.DocField
import cassdoc.FieldValue
import cassdoc.IDUtil
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelKey
import cassdoc.RelTypes
import cassdoc.TypeConfigurationService
import cassdoc.commands.retrieve.GetAttrCmd
import cassdoc.commands.retrieve.GetAttrMetaCmd
import cassdoc.commands.retrieve.GetAttrMetaRCH
import cassdoc.commands.retrieve.GetAttrRCH
import cassdoc.commands.retrieve.GetDoc
import cassdoc.commands.retrieve.GetDocAttrsRP
import cassdoc.commands.retrieve.GetDocRCH
import cassdoc.commands.retrieve.GetRelKeyCmd
import cassdoc.commands.retrieve.GetRelsCmd
import cassdoc.commands.retrieve.GetRelsRCH

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken

import cwdrg.lg.annotation.Log
import cwdrg.util.async.iterator.BlockingIterator
import cwdrg.util.json.JSONUtil


// TODO: make a service
// TODO: rewrite result set processing to stream the attribute rows...

/*
 - primary query result set is paged
 - secondary filters and followups execute as needed
 - if they hold up the next page processing, then so be it, we prevent over-retrieval and swamping downstream bottlenecks
 --- how do batch the followups, insert into multiple points? rel retreival is a folowup query...
 */

@Log
@CompileStatic
class RetrievalOperations {

    /**
     * Deserialize the provided docUUID's document as a Map
     *
     * @param svcs
     * @param opctx
     * @param detail
     * @param docUUID
     * @param root
     * @return
     */
    static Map<String, Object> deserializeSingleDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, boolean root) {
        Map<String, Object> map = [:]
        map[AttrNames.SYS_DOCID] = docUUID
        if (detail.docIDTimestampMeta) {
            map[AttrNames.META_IDTIME] = IDUtil.extractUnixTimeFromEaioTimeUUID(docUUID)
        }
        if (detail.docIDDateMeta) {
            map[AttrNames.META_IDDATE] = new Date(IDUtil.extractUnixTimeFromEaioTimeUUID(docUUID))
        }
        if (detail.docTokenMeta || detail.docPaxosMeta || detail.docPaxosTimestampMeta || detail.docPaxosDateMeta || detail.docMetaIDMeta || detail.parentMeta || detail.docWritetimeMeta != null || detail.docWritetimeDateMeta != null) {
            GetDoc eCmd = new GetDoc(docUUID: docUUID)
            GetDocRCH eRCH = eCmd.queryCassandra(svcs, opctx, detail)
            if (detail.docTokenMeta) {
                map[AttrNames.META_TOKEN] = eRCH.token
            }
            if (detail.docPaxosMeta) {
                map[AttrNames.META_PAXOS] = eRCH.paxosVer.toString()
            }
            if (detail.docPaxosTimestampMeta) {
                map[AttrNames.META_PAXOSTIME] = IDUtil.extractUnixTimeFromEaioTimeUUID(eRCH.paxosVer.toString())
            }
            if (detail.docPaxosDateMeta) {
                map[AttrNames.META_PAXOSDATE] = IDUtil.extractUnixTimeFromEaioTimeUUID(eRCH.paxosVer.toString())
            }
            if (detail.docMetaIDMeta) {
                map[AttrNames.META_DOCMETAID] = eRCH.metadata_id
            }
            if (detail.docMetaDataMeta) {
                if (eRCH.metadata_id != null) {
                    Detail metaDetail = detail.resolveAttrDetail(eRCH.metadata_id)
                    map[AttrNames.META_DOCMETADATA] = deserializeSingleDoc(svcs, opctx, metaDetail, eRCH.metadata_id, false)
                }
            }
            if (detail.parentMeta) {
                map[AttrNames.META_PARENT] = eRCH.a0
            }
            if (detail.docWritetimeMeta) {
                map[AttrNames.META_WT_PRE + StringEscapeUtils.escapeJson(detail.docWritetimeMeta) + "]"] = eRCH.writetime
            }
            if (detail.docWritetimeDateMeta) {
                map[AttrNames.META_WTDT_PRE + StringEscapeUtils.escapeJson(detail.docWritetimeMeta) + "]"] = eRCH.writetime.intdiv(1000)
            }
        }
        if (detail.docRelationsMeta) {
            GetRelsCmd rels = new GetRelsCmd(p1: docUUID)
            GetRelsRCH relRCH = rels.queryCassandraDocRels(svcs, opctx, detail)
            map[AttrNames.META_RELS] = relRCH.rels
        }
        if (detail.docChildrenMeta) {
            GetRelsCmd rels = new GetRelsCmd(p1: docUUID, ty1: RelTypes.TO_CHILD)
            GetRelsRCH relRCH = rels.queryCassandraDocRelsForType(svcs, opctx, detail)
            map[AttrNames.META_CHILDREN] = relRCH.rels
        }
        // nonstreaming
        //GetDocAttrs cmd = new GetDocAttrs(docUUID:docUUID)
        //GetDocAttrsRCH rch = cmd.queryCassandra(svcs,opctx,detail)
        //for (Object[] attr : rch.attrs) {
        GetDocAttrsRP cmd = new GetDocAttrsRP(docUUID: docUUID)
        cmd.initiateQuery(svcs, opctx, detail)
        Object[] attr = null
        while (attr = cmd.nextRow()) {

            Detail attrDetail = detail.resolveAttrDetail((String) attr[0])
            if (attrDetail != null) {
                // if attr-specific detail meta differs from the base detail we used to query doc attrs, we need to do a followup query
                if (attrDetail.attrWritetimeMeta != detail.attrWritetimeMeta || attrDetail.attrTokenMeta != detail.attrTokenMeta
                        || attrDetail.attrMetaIDMeta != detail.attrMetaIDMeta || attrDetail.attrMetaDataMeta != detail.attrMetaDataMeta) {

                    GetAttrMetaCmd metaCmd = new GetAttrMetaCmd(docUUID: docUUID, attrName: (String) attr[0])
                    GetAttrMetaRCH metaRCH = metaCmd.queryCassandra(svcs, opctx, attrDetail)
                    // overwrite/fill in with correct detail values
                    attr[4] = metaRCH.writetime
                    attr[5] = metaRCH.token
                    attr[6] = metaRCH.attrMetaID
                }
                String keyname = attr[0]
                Object value = null
                if (attr[1] == null) {
                    value = null
                } else if (attr[1] == DBCodes.TYPE_CODE_STRING) {
                    value = attr[2]
                } else if (attr[1] == DBCodes.TYPE_CODE_INTEGER) {
                    value = new BigInteger((String) attr[2])
                } else if (attr[1] == DBCodes.TYPE_CODE_BOOLEAN) {
                    value = Boolean.parseBoolean((String) attr[2])
                } else if (attr[1] == DBCodes.TYPE_CODE_DECIMAL) {
                    value = new BigDecimal((String) attr[2])
                } else if (attr[1] == DBCodes.TYPE_CODE_ARRAY) {
                    JsonParser arrayParser = svcs.jsonFactory.createParser((String) attr[2])
                    JsonToken arrayStartToken = arrayParser.nextToken()
                    if (arrayStartToken == JsonToken.START_ARRAY) {
                        value = deserializeRetrievedChildArray(svcs, opctx, attrDetail, docUUID, (String) attr[0], arrayParser)
                    } else {
                        // array type but not array? check for empty string or null
                    }
                } else if (attr[1] == DBCodes.TYPE_CODE_OBJECT) {
                    JsonParser objParser = svcs.jsonFactory.createParser((String) attr[2])
                    JsonToken objStartToken = objParser.nextToken()
                    if (objStartToken == JsonToken.START_OBJECT) {
                        value = deserializeRetrievedChildObject(svcs, opctx, attrDetail, docUUID, (String) attr[0], objParser)
                    } else {
                        // obj type but no start object? check for empty string or null
                    }
                } else {
                    throw log.err(opctx, null, new InvalidTypeException("GETDOC_BADTYPE: DocUUID $docUUID has unknown attr type code ${attr[1]} for attr ${attr[0]}"))
                }

                if (keyname != null) {
                    map[keyname] = value
                }
                if (attrDetail.attrWritetimeMeta != null) {
                    map[((String) attr[0]) + AttrNames.META_WT_PRE + detail.attrWritetimeMeta + ']'] = attr[4]
                }
                if (attrDetail.attrWritetimeDateMeta) {
                    map[((String) attr[0]) + AttrNames.META_WTDT_PRE + attrDetail.attrWritetimeMeta + ']'] = new Date((Long)((Long) attr[4]).intdiv(1000))
                }
                if (attrDetail.attrTokenMeta) {
                    map[((String) attr[0]) + AttrNames.META_TOKEN] = attr[5]
                }
                if (attrDetail.attrPaxosMeta) {
                    map[((String) attr[0]) + AttrNames.META_PAXOS] = attr[3]
                }
                if (attrDetail.attrPaxosTimestampMeta) {
                    map[((String) attr[0]) + AttrNames.META_PAXOSTIME] = IDUtil.extractUnixTimeFromEaioTimeUUID(attr[3].toString())
                }
                if (attrDetail.attrPaxosDateMeta) {
                    map[((String) attr[0]) + AttrNames.META_PAXOSDATE] = new Date(IDUtil.extractUnixTimeFromEaioTimeUUID(attr[3].toString())).toGMTString()
                }
                if (attrDetail.attrMetaIDMeta) {
                    map[((String) attr[0]) + AttrNames.META_ATTRMETAID] = attr[6]
                }
                if (attrDetail.attrMetaDataMeta) {
                    if (attr[6] != null) {
                        Detail metaDetail = detail.resolveAttrDetail((String) attr[6])
                        map[((String) attr[0]) + AttrNames.META_ATTRMETADATA] = deserializeSingleDoc(svcs, opctx, metaDetail, (String) attr[6], false)
                    }
                }

            } else {
                // log.debug: detail-excluded atribute
            }
        }
        if (root && opctx.cqlTraceEnabled) {
            map[AttrNames.META_CQLTRACE] = opctx.cqlTrace
        }
        return map
    }

    static void getSingleDoc(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, Writer writer, boolean root) {
        if (root) {
            if (!query(svcs, opctx, detail, "SELECT token(e), zv from ${opctx.space}.e_${IDUtil.idSuffix(docUUID)} WHERE e = ?", docUUID)) {
                throw new ResourceNotFoundException(docUUID)
            }
        }
        writer << '{"' << AttrNames.SYS_DOCID << '":"' << docUUID << '"'
        if (detail.docIDTimestampMeta) {
            writer << ',"' << AttrNames.META_IDTIME << '":' << IDUtil.extractUnixTimeFromEaioTimeUUID(docUUID)
        }
        if (detail.docIDDateMeta) {
            writer << ',"' << AttrNames.META_IDDATE << '":"' << new Date(IDUtil.extractUnixTimeFromEaioTimeUUID(docUUID)).toGMTString() << '"'
        }
        if (detail.docTokenMeta || detail.docPaxosMeta || detail.docPaxosTimestampMeta || detail.docPaxosDateMeta || detail.docMetaIDMeta || detail.parentMeta || detail.docWritetimeMeta != null || detail.docWritetimeDateMeta != null) {
            GetDoc eCmd = new GetDoc(docUUID: docUUID)
            GetDocRCH eRCH = eCmd.queryCassandra(svcs, opctx, detail)
            if (detail.docTokenMeta) {
                writer << ',"' << AttrNames.META_TOKEN << '":' << eRCH.token
            }
            if (detail.docPaxosMeta) {
                writer << ',"' << AttrNames.META_PAXOS << '":"' << eRCH.paxosVer.toString() << '"'
            }
            if (detail.docPaxosTimestampMeta) {
                writer << ',"' << AttrNames.META_PAXOSTIME << '":' << IDUtil.extractUnixTimeFromEaioTimeUUID(eRCH.paxosVer.toString())
            }
            if (detail.docPaxosDateMeta) {
                writer << ',"' << AttrNames.META_PAXOSDATE << '":"' << new Date(IDUtil.extractUnixTimeFromEaioTimeUUID(eRCH.paxosVer.toString())).toGMTString() << '"'
            }
            if (detail.docMetaIDMeta) {
                writer << ',"' << AttrNames.META_DOCMETAID << '":"' << JSONUtil.serialize(eRCH.metadata_id) << '"'
            }
            if (detail.docMetaDataMeta) {
                if (eRCH.metadata_id != null) {
                    Detail metaDetail = detail.resolveAttrDetail(eRCH.metadata_id)
                    writer << ',"' << AttrNames.META_DOCMETADATA << '":'
                    getSingleDoc(svcs, opctx, metaDetail, eRCH.metadata_id, writer, false)
                }
            }
            if (detail.parentMeta) {
                writer << ',"' << AttrNames.META_PARENT << '":"' << eRCH.a0 << '"'
            }
            if (detail.docWritetimeMeta) {
                writer << ',"' << AttrNames.META_WT_PRE << StringEscapeUtils.escapeJson(detail.docWritetimeMeta) + ']":' << eRCH.writetime
            }
            if (detail.docWritetimeDateMeta) {
                writer << ',"' << AttrNames.META_WTDT_PRE << StringEscapeUtils.escapeJson(detail.docWritetimeMeta) + ']":"' << new Date((Long)eRCH.writetime.intdiv(1000)).toGMTString() << '"'
            }
        }
        if (detail.docRelationsMeta) {
            GetRelsCmd rels = new GetRelsCmd(p1: docUUID)
            GetRelsRCH relRCH = rels.queryCassandraDocRels(svcs, opctx, detail)
            writer << ',"' << AttrNames.META_RELS << '":' << JSONUtil.serialize(relRCH.rels)
        }
        if (detail.docChildrenMeta) {
            GetRelsCmd rels = new GetRelsCmd(p1: docUUID, ty1: RelTypes.TO_CHILD)
            GetRelsRCH relRCH = rels.queryCassandraDocRelsForType(svcs, opctx, detail)
            writer << ',"' << AttrNames.META_CHILDREN << '":' << JSONUtil.serialize(relRCH.rels)
        }
        // nonstreaming
        //GetDocAttrs cmd = new GetDocAttrs(docUUID:docUUID)
        //GetDocAttrsRCH rch = cmd.queryCassandra(svcs,opctx,detail)
        //for (Object[] attr : rch.attrs) {

        GetDocAttrsRP cmd = new GetDocAttrsRP(docUUID: docUUID)
        cmd.initiateQuery(svcs, opctx, detail)
        Object[] attr = null
        while (attr = cmd.nextRow()) {

            Detail attrDetail = detail.resolveAttrDetail((String) attr[0])
            if (attrDetail != null) {
                // if attr-specific detail meta differs from the base detail we used to query doc attrs, we need to do a followup query
                if (attrDetail.attrWritetimeMeta != detail.attrWritetimeMeta || attrDetail.attrTokenMeta != detail.attrTokenMeta
                        || attrDetail.attrMetaIDMeta != detail.attrMetaIDMeta || attrDetail.attrMetaDataMeta != detail.attrMetaDataMeta) {
                    GetAttrMetaCmd metaCmd = new GetAttrMetaCmd(docUUID: docUUID, attrName: (String) attr[0])
                    GetAttrMetaRCH metaRCH = metaCmd.queryCassandra(svcs, opctx, attrDetail)
                    // overwrite/fill in with correct detail values
                    attr[4] = metaRCH.writetime
                    attr[5] = metaRCH.token
                    attr[6] = metaRCH.attrMetaID
                }
                writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << '":'
                if (attr[1] == DBCodes.TYPE_CODE_STRING) {
                    writer << '"' << StringEscapeUtils.escapeJson((String) attr[2]) << '"'
                } else if (attr[1] == null || attr[1] == DBCodes.TYPE_CODE_INTEGER || attr[1] == DBCodes.TYPE_CODE_DECIMAL || attr[1] == DBCodes.TYPE_CODE_BOOLEAN) {
                    writer << attr[2]
                } else if (attr[1] == DBCodes.TYPE_CODE_ARRAY) {
                    JsonParser arrayParser = svcs.jsonFactory.createParser((String) attr[2])
                    JsonToken arrayStartToken = arrayParser.nextToken()
                    if (arrayStartToken == JsonToken.START_ARRAY) {
                        parseRetrievedChildArray(svcs, opctx, attrDetail, docUUID, (String) attr[0], arrayParser, writer)
                    } else {
                        // array type but not array? check for empty string or null
                    }
                } else if (attr[1] == DBCodes.TYPE_CODE_OBJECT) {
                    JsonParser objParser = svcs.jsonFactory.createParser((String) attr[2])
                    JsonToken objStartToken = objParser.nextToken()
                    if (objStartToken == JsonToken.START_OBJECT) {
                        parseRetrievedChildObject(svcs, opctx, attrDetail, docUUID, (String) attr[0], objParser, writer)
                    } else {
                        // obj type but no start object? check for empty string or null
                    }
                } else {
                    throw log.err(opctx, null, new InvalidTypeException("GETDOC_BADTYPE: DocUUID $docUUID has unknown attr type code ${attr[1]} for attr ${attr[0]}"))
                }
                if (attrDetail.attrWritetimeMeta != null) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_WT_PRE << detail.attrWritetimeMeta << ']":' << attr[4]
                }
                if (attrDetail.attrWritetimeDateMeta) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_WTDT_PRE + StringEscapeUtils.escapeJson(attrDetail.attrWritetimeMeta) + ']":"' << new Date((Long)((Long) attr[4]).intdiv(1000)).toGMTString() << '"'
                }
                if (attrDetail.attrTokenMeta) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_TOKEN << '":' << attr[5]
                }
                if (attrDetail.attrPaxosMeta) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_PAXOS << '":"' << attr[3] << '"'
                }
                if (attrDetail.attrPaxosTimestampMeta) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_PAXOSTIME << '":' << IDUtil.extractUnixTimeFromEaioTimeUUID(attr[3].toString())
                }
                if (attrDetail.attrPaxosDateMeta) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_PAXOSDATE << '":"' << new Date(IDUtil.extractUnixTimeFromEaioTimeUUID(attr[3].toString())).toGMTString() << '"'
                }
                if (attrDetail.attrMetaIDMeta) {
                    writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_ATTRMETAID << '":"' << attr[6] << '"'
                }
                if (attrDetail.attrMetaDataMeta) {
                    if (attr[6] != null) {
                        Detail metaDetail = detail.resolveAttrDetail((String) attr[6])
                        writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << AttrNames.META_ATTRMETADATA << '":'
                        getSingleDoc(svcs, opctx, metaDetail, (String) attr[0], writer, false)
                    }
                }
            } else {
                // log.debug: detail-excluded atribute
            }
        }
        if (root && opctx.cqlTraceEnabled) {
            writer << ',"' << AttrNames.META_CQLTRACE << '":' << JSONUtil.serialize(opctx.cqlTrace)
        }
        writer << "}"
    }


    static void getAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr, Writer writer) {
        // writer << '"' << StringEscapeUtils.escapeJson(attr) << '":' // retiring...
        GetAttrCmd cmd = new GetAttrCmd(docUUID: docUUID, attrName: attr)
        GetAttrRCH rch = cmd.queryCassandra(svcs, opctx, detail)
        if (rch.valType == DBCodes.TYPE_CODE_STRING) {
            writer << '"' << StringEscapeUtils.escapeJson(rch.data) << '"'
        } else if (rch.valType == null || rch.valType == DBCodes.TYPE_CODE_DECIMAL || rch.valType == DBCodes.TYPE_CODE_BOOLEAN || rch.valType == DBCodes.TYPE_CODE_INTEGER) {
            writer << rch.data
        } else if (rch.valType == DBCodes.TYPE_CODE_ARRAY) {
            JsonParser arrayParser = svcs.jsonFactory.createParser(rch.data)
            JsonToken arrayStartToken = arrayParser.nextToken()
            if (arrayStartToken == JsonToken.START_ARRAY) {
                parseRetrievedChildArray(svcs, opctx, detail, docUUID, attr, arrayParser, writer)
            } else {
                // array type but not array? check for empty string or null
            }
        } else if (rch.valType == DBCodes.TYPE_CODE_OBJECT) {
            JsonParser objParser = svcs.jsonFactory.createParser(rch.data)
            JsonToken objStartToken = objParser.nextToken()
            if (objStartToken == JsonToken.START_OBJECT) {
                parseRetrievedChildObject(svcs, opctx, detail, docUUID, attr, objParser, writer)
            } else {
                // obj type but no start object? check for empty string or null
            }
        } else {
            // error
        }
    }

    static Object deserializeAttr(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr) {
        GetAttrCmd cmd = new GetAttrCmd(docUUID: docUUID, attrName: attr)
        GetAttrRCH rch = cmd.queryCassandra(svcs, opctx, detail)
        if (rch.data == null) {
            return null
        }
        if (rch.valType == DBCodes.TYPE_CODE_STRING) {
            return rch.data
        }
        if (rch.valType == DBCodes.TYPE_CODE_DECIMAL) {
            return new BigDecimal(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_BOOLEAN) {
            return Boolean.parseBoolean(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_INTEGER) {
            return new BigInteger(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_ARRAY) {
            JsonParser arrayParser = svcs.jsonFactory.createParser(rch.data)
            JsonToken arrayStartToken = arrayParser.nextToken()
            if (arrayStartToken == JsonToken.START_ARRAY) {
                return deserializeRetrievedChildArray(svcs, opctx, detail, docUUID, attr, arrayParser)
            }
        }
        if (rch.valType == DBCodes.TYPE_CODE_OBJECT) {
            JsonParser objParser = svcs.jsonFactory.createParser(rch.data)
            JsonToken objStartToken = objParser.nextToken()
            if (objStartToken == JsonToken.START_OBJECT) {
                return deserializeRetrievedChildObject(svcs, opctx, detail, docUUID, attr, objParser)
            }
        }
        return null
    }


    static DocField getDocField(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr) {
        DocField docfield = new DocField(docUUID: docUUID, name: attr)
        StringWriter writer = new StringWriter()
        GetAttrCmd cmd = new GetAttrCmd(docUUID: docUUID, attrName: attr)
        GetAttrRCH rch = cmd.queryCassandra(svcs, opctx, detail)
        if (rch.valType == DBCodes.TYPE_CODE_STRING) {
            writer << '"' << StringEscapeUtils.escapeJson(rch.data) << '"'
        } else if (rch.valType == null || rch.valType == DBCodes.TYPE_CODE_DECIMAL || rch.valType == DBCodes.TYPE_CODE_BOOLEAN || rch.valType == DBCodes.TYPE_CODE_INTEGER) {
            writer << rch.data
        } else if (rch.valType == DBCodes.TYPE_CODE_ARRAY) {
            JsonParser arrayParser = svcs.jsonFactory.createParser(rch.data)
            JsonToken arrayStartToken = arrayParser.nextToken()
            if (arrayStartToken == JsonToken.START_ARRAY) {
                parseRetrievedChildArray(svcs, opctx, detail, docUUID, attr, arrayParser, writer)
            } else {
                // array type but not array? check for empty string or null
            }
        } else if (rch.valType == DBCodes.TYPE_CODE_OBJECT) {
            JsonParser objParser = svcs.jsonFactory.createParser(rch.data)
            JsonToken objStartToken = objParser.nextToken()
            if (objStartToken == JsonToken.START_OBJECT) {
                parseRetrievedChildObject(svcs, opctx, detail, docUUID, attr, objParser, writer)
            } else {
                // obj type but no start object? check for empty string or null
            }
        } else {
            // error
        }
        docfield.value = new FieldValue(value: writer.toString(), type: TypeConfigurationService.attrClass(rch.valType))
        return docfield
    }

    static void parseRetrievedChildArray(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, JsonParser arrayParser, Writer writer) {
        writer << '['
        boolean firstMember = true
        while (true) {
            JsonToken token = arrayParser.nextToken()
            if (token == JsonToken.END_ARRAY) {
                writer << ']'
                // unrecurse
                return
            }

            if (firstMember) {
                firstMember = false
            } else {
                writer << ','
            }

            if (token == JsonToken.VALUE_NULL) {
                writer << "null"
            }
            if (token == JsonToken.VALUE_TRUE) {
                writer << "true"
            }
            if (token == JsonToken.VALUE_FALSE) {
                writer << "false"
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                writer << arrayParser.getText()
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                writer << arrayParser.getText()
            }
            if (token == JsonToken.VALUE_STRING) {
                writer << '"' << StringEscapeUtils.escapeJson(arrayParser.getText()) << '"'
            }

            if (token == JsonToken.START_ARRAY) {
                // recurse
                parseRetrievedChildArray(svcs, opctx, detail, docUUID, attrName, arrayParser, writer)
            }

            if (token == JsonToken.START_OBJECT) {
                // recurse
                parseRetrievedChildObject(svcs, opctx, detail, docUUID, attrName, arrayParser, writer)
            }
        }
    }


    static List deserializeRetrievedChildArray(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, JsonParser arrayParser) {
        List<Object> list = []
        while (true) {
            JsonToken token = arrayParser.nextToken()
            if (token == JsonToken.END_ARRAY) {
                return list
            }

            if (token == JsonToken.VALUE_NULL) {
                list.add(null)
            }
            if (token == JsonToken.VALUE_TRUE) {
                list.add(true)
            }
            if (token == JsonToken.VALUE_FALSE) {
                list.add(false)
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                list.add(new BigDecimal(arrayParser.getText()))
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                list.add(new BigInteger(arrayParser.getText()))
            }
            if (token == JsonToken.VALUE_STRING) {
                list.add(arrayParser.getText())
            }

            if (token == JsonToken.START_ARRAY) {
                // recurse
                list.add(deserializeRetrievedChildArray(svcs, opctx, detail, docUUID, attrName, arrayParser))
            }

            if (token == JsonToken.START_OBJECT) {
                // recurse
                list.add(deserializeRetrievedChildObject(svcs, opctx, detail, docUUID, attrName, arrayParser))
            }
        }
    }

    static void parseRetrievedChildObject(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, JsonParser objParser, Writer writer) {

        boolean firstField = true
        String currentField = null
        Detail objectDetail = detail     // if it's a docref, probably will be a different detail level...
        while (true) {
            JsonToken token = objParser.nextToken()
            if (token == JsonToken.FIELD_NAME) {
                currentField = objParser.getCurrentName()
                if (firstField) {
                    if (svcs.idField.equals(currentField)) {
                        String childUUID = parseRetrievedIDField(opctx, detail, objParser)
                        objectDetail = detail.resolveChildDocDetail(childUUID, attrName)
                        if (objectDetail != null) {
                            getSingleDoc(svcs, opctx, objectDetail, childUUID, writer, false)
                            // wiuth a DBREF like this, we should have a END_OBJECT event to consume
                            JsonToken excessEndObjectToken = objParser.nextToken()
                            if (excessEndObjectToken != JsonToken.END_OBJECT) {
                                throw new IllegalArgumentException("ERROR in parse: wrong excess token: " + excessEndObjectToken)
                            }
                            return
                        } else {
                            writer << '{"' << AttrNames.SYS_DOCID << '":"' << StringEscapeUtils.escapeJson(childUUID) << '"}'
                            JsonToken excessEndObjectToken = objParser.nextToken()
                            if (excessEndObjectToken != JsonToken.END_OBJECT) {
                                throw new IllegalArgumentException("ERROR in parse: wrong excess token after stub: " + excessEndObjectToken)
                            }
                            return
                        }
                    } else {
                        firstField = false
                        writer << '{'
                    }
                } else {
                    writer << ","
                }
                writer << '"' << StringEscapeUtils.escapeJson(currentField) << '":'
            } else if (token == JsonToken.VALUE_NULL) {
                writer << "null"
            } else if (token == JsonToken.VALUE_TRUE) {
                writer << "true"
            } else if (token == JsonToken.VALUE_FALSE) {
                writer << "false"
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                writer << objParser.getText()
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                writer << objParser.getText()
            } else if (token == JsonToken.VALUE_STRING) {
                writer << '"' << StringEscapeUtils.escapeJson(objParser.getText()) << '"'
            } else if (token == JsonToken.START_ARRAY) {
                // recurse
                parseRetrievedChildArray(svcs, opctx, objectDetail, docUUID, attrName, objParser, writer)
            } else if (token == JsonToken.START_OBJECT) {
                // recurse (should be a non-child-doc)
                parseRetrievedChildObject(svcs, opctx, objectDetail, docUUID, attrName, objParser, writer)
            } else if (token == JsonToken.END_OBJECT) {
                // unrecurse...
                writer << '}'
                return
            }
        }
    }

    static Map deserializeRetrievedChildObject(CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attrName, JsonParser objParser) {
        Map map = [:]
        boolean firstField = true
        String currentField = null
        Detail objectDetail = detail     // if it's a docref, probably will be a different detail level...
        while (true) {
            JsonToken token = objParser.nextToken()
            if (token == JsonToken.FIELD_NAME) {
                currentField = objParser.getCurrentName()
                if (firstField) {
                    if (svcs.idField.equals(currentField)) {
                        String childUUID = parseRetrievedIDField(opctx, detail, objParser)
                        objectDetail = detail.resolveChildDocDetail(childUUID, attrName)
                        if (objectDetail != null) {
                            map = deserializeSingleDoc(svcs, opctx, objectDetail, childUUID, false)
                            // wiuth a DBREF like this, we should have a END_OBJECT event to consume
                            JsonToken excessEndObjectToken = objParser.nextToken()
                            if (excessEndObjectToken != JsonToken.END_OBJECT) {
                                throw new IllegalArgumentException("ERROR in parse: wrong excess token: " + excessEndObjectToken)
                            }
                            return map
                        } else {
                            map[AttrNames.SYS_DOCID] = childUUID
                            JsonToken excessEndObjectToken = objParser.nextToken()
                            if (excessEndObjectToken != JsonToken.END_OBJECT) {
                                throw new IllegalArgumentException("ERROR in parse: wrong excess token after stub: " + excessEndObjectToken)
                            }
                            return map
                        }
                    } else {
                        firstField = false
                    }
                }
            } else if (token == JsonToken.VALUE_NULL) {
                map[currentField] = null
            } else if (token == JsonToken.VALUE_TRUE) {
                map[currentField] = true
            } else if (token == JsonToken.VALUE_FALSE) {
                map[currentField] = false
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                map[currentField] = new BigDecimal(objParser.getText())
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                map[currentField] = new BigInteger(objParser.getText())
            } else if (token == JsonToken.VALUE_STRING) {
                map[currentField] = objParser.getText()
            } else if (token == JsonToken.START_ARRAY) {
                // recurse
                map[currentField] = deserializeRetrievedChildArray(svcs, opctx, objectDetail, docUUID, attrName, objParser)
            } else if (token == JsonToken.START_OBJECT) {
                // recurse (should be a non-child-doc)
                map[currentField] = deserializeRetrievedChildObject(svcs, opctx, objectDetail, docUUID, attrName, objParser)
            } else if (token == JsonToken.END_OBJECT) {
                // unrecurse...
                return map
            }
        }
    }

    // extract or generate UUID
    static String parseRetrievedIDField(OperationContext opctx, Detail detail, JsonParser parser) {
        JsonToken token = parser.nextToken()
        if (token == JsonToken.VALUE_STRING) {
            String idString = parser.getText()
            return idString
        }
        // TODO: more complicated stuff? DFRef object or something similar?
        throw new IllegalArgumentException("ID information not retrievable")
    }

    /**
     * This provides a streaming object iterator so that large documents in the database can be streamed as Map.Entry objects aka key-value,
     * as if one were iterating through the key-value entries of Map.entrySet.iterator()
     *
     * JSON value types are as follows:
     * - String values are strings
     * - floats/decimals are BigDecimals
     * - integers are BigIntegers
     * - null is null
     * - subdocuments are Maps
     * - arrays are Lists
     *
     * @author cowardlydragon
     *
     */
    static Iterator<Map.Entry<String, Object>> deserializeAttrIterator(
            final CommandExecServices svcs,
            final OperationContext opctx, final Detail detail, final String docUUID, final boolean root) {

        final BlockingQueue attrQ = new LinkedBlockingQueue(1000)
        final BlockingIterator<Map.Entry<String, Object>> iterator = new BlockingIterator<Map.Entry<String, Object>>(queue: attrQ)
        // fire off a thread?
        Map.Entry<String, Object> docUUIDField = new AbstractMap.SimpleEntry<String, Object>(AttrNames.SYS_DOCID, docUUID)
        attrQ.put(docUUIDField)

        new Thread() {
            void run() {
                if (detail.docIDTimestampMeta) {
                    attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_IDTIME, IDUtil.extractUnixTimeFromEaioTimeUUID(docUUID)))
                }
                if (detail.docIDDateMeta) {
                    attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_IDDATE, IDUtil.extractUnixTimeFromEaioTimeUUID(docUUID)))
                }
                if (detail.docTokenMeta || detail.docPaxosMeta || detail.docPaxosTimestampMeta || detail.docPaxosDateMeta || detail.docMetaIDMeta || detail.parentMeta || detail.docWritetimeMeta != null || detail.docWritetimeDateMeta != null) {
                    GetDoc eCmd = new GetDoc(docUUID: docUUID)
                    GetDocRCH eRCH = eCmd.queryCassandra(svcs, opctx, detail)
                    if (detail.docTokenMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_TOKEN, eRCH.token))
                    }
                    if (detail.docPaxosMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_PAXOS, eRCH.paxosVer))
                    }
                    if (detail.docPaxosTimestampMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_PAXOSTIME, IDUtil.extractUnixTimeFromEaioTimeUUID(eRCH.paxosVer.toString())))
                    }
                    if (detail.docPaxosDateMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_PAXOSDATE, IDUtil.extractUnixTimeFromEaioTimeUUID(eRCH.paxosVer.toString())))
                    }
                    if (detail.docMetaIDMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_DOCMETAID, eRCH.metadata_id))
                    }
                    if (detail.docMetaDataMeta) {
                        if (eRCH.metadata_id != null) {
                            Detail metaDetail = detail.resolveAttrDetail(eRCH.metadata_id)
                            Map metadatadoc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, metaDetail, eRCH.metadata_id, false)
                            attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_DOCMETADATA, metadatadoc))
                        }
                    }
                    if (detail.parentMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_PARENT, eRCH.a0))
                    }
                    if (detail.docWritetimeMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_WT_PRE + detail.docWritetimeMeta + "]", eRCH.writetime))
                    }
                    if (detail.docWritetimeDateMeta) {
                        attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_WTDT_PRE + detail.docWritetimeMeta + "]", eRCH.writetime.intdiv(1000)))
                    }
                }
                if (detail.docRelationsMeta) {
                    GetRelsCmd rels = new GetRelsCmd(p1: docUUID)
                    GetRelsRCH relRCH = rels.queryCassandraDocRels(svcs, opctx, detail)
                    attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_RELS, relRCH.rels))
                }
                if (detail.docChildrenMeta) {
                    GetRelsCmd rels = new GetRelsCmd(p1: docUUID, ty1: RelTypes.TO_CHILD)
                    GetRelsRCH relRCH = rels.queryCassandraDocRelsForType(svcs, opctx, detail)
                    attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_CHILDREN, relRCH.rels))
                }

                // non-streaming
                //GetDocAttrs cmd = new GetDocAttrs(docUUID:docUUID)
                //GetDocAttrsRCH rch = cmd.queryCassandra(svcs,opctx,detail)
                //for (Object[] attr : rch.attrs) {

                // streaming
                GetDocAttrsRP cmd = new GetDocAttrsRP(docUUID: docUUID)
                cmd.initiateQuery(svcs, opctx, detail)
                Object[] attr = null
                while (attr = cmd.nextRow()) {

                    Detail attrDetail = detail.resolveAttrDetail((String) attr[0])
                    // if attr-specific detail meta differs from the base detail we used to query doc attrs, we need to do a followup query
                    if (attrDetail.attrWritetimeMeta != detail.attrWritetimeMeta || attrDetail.attrTokenMeta != detail.attrTokenMeta
                            || attrDetail.attrMetaIDMeta != detail.attrMetaIDMeta || attrDetail.attrMetaDataMeta != detail.attrMetaDataMeta) {
                        GetAttrMetaCmd metaCmd = new GetAttrMetaCmd(docUUID: docUUID, attrName: (String) attr[0])
                        GetAttrMetaRCH metaRCH = metaCmd.queryCassandra(svcs, opctx, attrDetail)
                        // overwrite/fill in with correct detail values
                        attr[4] = metaRCH.writetime
                        attr[5] = metaRCH.token
                        attr[6] = metaRCH.attrMetaID
                    }
                    if (attrDetail != null) {
                        Object value = null
                        if (attr[2] == null) {
                            value = null
                        } else {
                            if (attr[1] == DBCodes.TYPE_CODE_STRING) {
                                value = (String) attr[2]
                            } else if (attr[1] == DBCodes.TYPE_CODE_INTEGER) {
                                value = new BigInteger((String) attr[2])
                            } else if (attr[1] == DBCodes.TYPE_CODE_DECIMAL) {
                                value = new BigDecimal((String) attr[2])
                            } else if (attr[1] == DBCodes.TYPE_CODE_BOOLEAN) {
                                value = Boolean.parseBoolean((String) attr[2])
                            } else if (attr[1] == DBCodes.TYPE_CODE_ARRAY) {
                                JsonParser arrayParser = svcs.jsonFactory.createParser((String) attr[2])
                                JsonToken arrayStartToken = arrayParser.nextToken()
                                if (arrayStartToken == JsonToken.START_ARRAY) {
                                    List list = RetrievalOperations.deserializeRetrievedChildArray(svcs, opctx, attrDetail, docUUID, (String) attr[0], arrayParser)
                                    value = list
                                } else {
                                    // array type but not array? check for empty string or null
                                }
                            } else if (attr[1] == DBCodes.TYPE_CODE_OBJECT) {
                                JsonParser objParser = svcs.jsonFactory.createParser((String) attr[2])
                                JsonToken objStartToken = objParser.nextToken()
                                if (objStartToken == JsonToken.START_OBJECT) {
                                    Map map = RetrievalOperations.deserializeRetrievedChildObject(svcs, opctx, attrDetail, docUUID, (String) attr[0], objParser)
                                    value = map
                                } else {
                                    // obj type but no start object? check for empty string or null
                                }
                            } else {
                                throw log.err(opctx, null, new InvalidTypeException("GETDOC_BADTYPE: DocUUID $docUUID has unknown attr type code ${attr[1]} for attr ${attr[0]}"))
                            }
                            attrQ.put(new AbstractMap.SimpleEntry<String, Object>((String) attr[0], value))

                            // attr-specific meta-attrs
                            if (attrDetail.attrWritetimeMeta != null) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_WT_PRE + detail.attrWritetimeMeta + ']', attr[4]))
                            }
                            if (attrDetail.attrWritetimeDateMeta) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_WTDT_PRE + attrDetail.attrWritetimeMeta + ']', ((Long) attr[4]).intdiv(1000)))
                            }
                            if (attrDetail.attrTokenMeta) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_TOKEN, attr[5]))
                            }
                            if (attrDetail.attrPaxosMeta) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_PAXOS, attr[3]))
                            }
                            if (attrDetail.attrPaxosTimestampMeta) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_PAXOSTIME, IDUtil.extractUnixTimeFromEaioTimeUUID(attr[3].toString())))
                            }
                            if (attrDetail.attrPaxosDateMeta) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_PAXOSDATE, IDUtil.extractUnixTimeFromEaioTimeUUID(attr[3].toString())))
                            }
                            if (attrDetail.attrMetaIDMeta) {
                                attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_ATTRMETAID, attr[6]))
                            }
                            if (attrDetail.attrMetaDataMeta) {
                                if (attr[6] != null) {
                                    Detail metaDetail = detail.resolveAttrDetail((String) attr[6])
                                    Map metadatadoc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, metaDetail, (String) attr[6], false)
                                    attrQ.put(new AbstractMap.SimpleEntry<String, Object>("" + (String) attr[0] + AttrNames.META_ATTRMETADATA, metadatadoc))
                                }
                            }

                        }
                    } else {
                        // log.debug: detail-excluded atribute
                    }
                }
                if (root && opctx.cqlTraceEnabled) {
                    attrQ.put(new AbstractMap.SimpleEntry<String, Object>(AttrNames.META_CQLTRACE, opctx.cqlTrace))
                }
            }
        }.start()

        return iterator

    }

    static String getDocMetadataUUID(
            final CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID) {
        detail.docMetaIDMeta = true
        GetDoc eCmd = new GetDoc(docUUID: docUUID)
        GetDocRCH eRCH = eCmd.queryCassandra(svcs, opctx, detail)
        return eRCH.metadata_id
    }

    static String getAttrMetadataUUID(
            final CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID, String attr) {
        detail.attrMetaIDMeta = true
        GetAttrMetaCmd metaCmd = new GetAttrMetaCmd(docUUID: docUUID, attrName: attr)
        GetAttrMetaRCH metaRCH = metaCmd.queryCassandra(svcs, opctx, detail)
        return metaRCH.attrMetaID
    }

    static String getRelMetadataUUID(
            final CommandExecServices svcs, OperationContext opctx, Detail detail, RelKey relKey) {
        GetRelKeyCmd relkeyCmd = new GetRelKeyCmd(relKey: relKey)
        GetRelsRCH relCmd = relkeyCmd.queryCassandraRelKey(svcs, opctx, detail)
        if (relCmd.rels.size() == 1) {
            return relCmd.rels[0].z_md
        }
        throw new RuntimeException("RelKey not found during metadata id retrieval for " + JSONUtil.serialize(relKey))
    }

    static Rel getRel(final CommandExecServices svcs, OperationContext opctx, Detail detail, RelKey relKey) {
        GetRelKeyCmd relkeyCmd = new GetRelKeyCmd(relKey: relKey)
        GetRelsRCH relCmd = relkeyCmd.queryCassandraRelKey(svcs, opctx, detail)
        if (relCmd.rels.size() == 1) {
            return relCmd.rels[0]
        }
        throw new RuntimeException("RelKey not found during relation retrieval for " + JSONUtil.serialize(relKey))
    }

    static List<Rel> deserializeDocRels(
            final CommandExecServices svcs, OperationContext opctx, Detail detail, String docUUID) {
        GetRelsCmd rels = new GetRelsCmd(p1: docUUID)
        GetRelsRCH relRCH = rels.queryCassandraDocRels(svcs, opctx, detail)
        return relRCH.rels
    }

    static List<Object[]> query(CommandExecServices svcs, OperationContext opctx, Detail detail, String cql, Object[] args) {
        QueryToListOfStrArr cmd = new QueryToListOfStrArr(query: cql)
        if (args != null)
            cmd.initiateQuery(svcs, opctx, detail, args)
        else
            cmd.initiateQuery(svcs, opctx, detail)
        List queryresult = []
        Object[] data = null
        while (true) {
            data = cmd.nextRow()
            if (data == null) break
            queryresult.add(data)
        }
        return queryresult
    }

}

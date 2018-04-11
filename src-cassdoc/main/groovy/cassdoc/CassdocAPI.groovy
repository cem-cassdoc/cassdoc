package cassdoc

import cassdoc.config.CassDocConfig
import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringEscapeUtils
import org.springframework.beans.factory.annotation.Autowired

import cassdoc.commands.mutate.UpdAttrMetadata
import cassdoc.commands.mutate.UpdDocMetadata
import cassdoc.commands.mutate.UpdRelMetadata
import cassdoc.commands.retrieve.GetAttrCmd
import cassdoc.commands.retrieve.GetAttrRCH
import cassdoc.commands.retrieve.GetDocAttrs
import cassdoc.commands.retrieve.GetDocAttrsRCH
import cassdoc.commands.retrieve.QueryToListOfStrArr
import cassdoc.operations.CreateOperations
import cassdoc.operations.DeleteOperations
import cassdoc.operations.RetrievalOperations
import cassdoc.operations.UpdateOperations

import com.jayway.jsonpath.JsonPath

import cwdrg.lg.annotation.Log
import cwdrg.util.json.JSONUtil
import org.springframework.stereotype.Component

@Log
@CompileStatic
@Component
class CassdocAPI {

    // TODO: cross-space registry (id : space) <-- huh?

    @Autowired
    CassDocConfig config

    @Autowired
    CommandExecServices svcs

    // ---- existence checks

    boolean docExists(OperationContext opctx, Detail detail, String uuid) {
        // token would work too, at least on cass 3.5
        String typeSuffix = IDUtil.idSuffix(uuid)
        List<Object[]> rows = query(opctx, detail, "SELECT token(e),zv from ${opctx.space}.e_${typeSuffix} WHERE e = ?", uuid)
        return (rows.size() > 0)
    }

    boolean attrExists(OperationContext opctx, Detail detail, String uuid, String attr) {
        String typeSuffix = IDUtil.idSuffix(uuid)
        List<Object[]> rows = query(opctx, detail, "SELECT token(e),zv from ${opctx.space}.p_${typeSuffix} WHERE e = ? and p = ?", uuid, attr)
        return (rows.size() > 0)
    }

    // ---- Streaming Parse Read Operations

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
    String getSimpleAttr(OperationContext opctx, Detail detail, String docUUID, String attr) {

        StringWriter writer = new StringWriter()
        getSimpleAttr(opctx, detail, docUUID, attr, writer)
        String toStr = writer
        log.dbg('OPGetAttrSimple_return: ' + toStr, null)
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
    void getSimpleAttr(OperationContext opctx, Detail detail, String docUUID, String attr, Writer writer) {
        log.inf("OPGetAttrSimple_top :: $docUUID $attr", null)

        GetAttrCmd cmd = new GetAttrCmd(docUUID: docUUID, attrName: attr)
        GetAttrRCH rch = cmd.queryCassandra(svcs, opctx, detail)

        if (rch.valType == DBCodes.TYPE_CODE_STRING) {
            writer << '"' << StringEscapeUtils.escapeJson(rch.data) << '"'
        } else {
            writer << rch.data
        }
    }

    Object deserializeSimpleAttr(OperationContext opctx, Detail detail, String docUUID, String attr) {

        GetAttrCmd cmd = new GetAttrCmd(docUUID: docUUID, attrName: attr)
        GetAttrRCH rch = cmd.queryCassandra(svcs, opctx, detail)
        if (rch.data == null) {
            return null
        }
        if (rch.valType == DBCodes.TYPE_CODE_OBJECT) {
            return JSONUtil.deserializeMap(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_ARRAY) {
            return JSONUtil.deserializeList(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_STRING) {
            return rch.data
        }
        if (rch.valType == DBCodes.TYPE_CODE_BOOLEAN) {
            return Boolean.parseBoolean(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_INTEGER) {
            return new BigInteger(rch.data)
        }
        if (rch.valType == DBCodes.TYPE_CODE_DECIMAL) {
            return new BigDecimal(rch.data)
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
    String getSimpleDoc(OperationContext opctx, Detail detail, String docUUID) {
        StringWriter writer = new StringWriter()
        getSimpleDoc(opctx, detail, docUUID, writer)
        return writer.toString()
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
    void getSimpleDoc(OperationContext opctx, Detail detail, String docUUID, Writer writer) {
        writer << '{"_id":"' << docUUID << '"'
        GetDocAttrs cmd = new GetDocAttrs(docUUID: docUUID)
        GetDocAttrsRCH rch = cmd.queryCassandra(svcs, opctx, detail)
        for (Object[] attr : rch.attrs) {
            writer << ',"' << StringEscapeUtils.escapeJson((String) attr[0]) << '":'
            if (attr[1] == DBCodes.TYPE_CODE_STRING) {
                writer << '"' << StringEscapeUtils.escapeJson((String) attr[2]) << '"'
            } else {
                writer << attr[2]
            }
        }
        writer << '}'
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
        StringWriter writer = new StringWriter()
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, docUUID, writer, true)
        return writer.toString()
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
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, docUUID, writer, true)
    }

    /**
     * Get a document deserialized into a Map
     *
     * @param opctx
     * @param detail
     * @param docUUID
     * @return
     */
    Map<String, Object> deserializeDoc(OperationContext opctx, Detail detail, String docUUID) {
        Map map = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, docUUID, true)
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
        StringWriter writer = new StringWriter()
        RetrievalOperations.getAttr(svcs, opctx, detail, docUUID, attr, writer)
        return writer.toString()
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
        RetrievalOperations.getAttr(svcs, opctx, detail, docUUID, attr, writer)
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
        Object attrVal = RetrievalOperations.deserializeAttr(svcs, opctx, detail, docUUID, attr)
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
        StringWriter writer = new StringWriter()
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, docUUID, writer, true)
        String json = writer
        JsonPath pathexpr = JsonPath.compile(jsonPath)
        return JsonPath.parse(json).read(pathexpr).toString()
    }

    void getDocJsonPath(OperationContext opctx, Detail detail, String docUUID, String jsonPath, Writer w) {
        StringWriter writer = new StringWriter()
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, docUUID, writer, true)
        String json = writer
        JsonPath pathexpr = JsonPath.compile(jsonPath)
        w << JsonPath.parse(json).read(pathexpr).toString()
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

        StringWriter writer = new StringWriter()
        RetrievalOperations.getAttr(svcs, opctx, detail, docUUID, attr, writer)
        String json = writer
        JsonPath pathexpr = JsonPath.compile(jsonPath)
        return JsonPath.parse(json).read(pathexpr).toString()
    }

    // TODO: jsonpath for getAttr
    void getAttrJsonPath(OperationContext opctx, Detail detail, String docUUID, String attr, String jsonPath, Writer w) {
        // TODO fully streaming version
        StringWriter writer = new StringWriter()
        RetrievalOperations.getAttr(svcs, opctx, detail, docUUID, attr, writer)
        String json = writer
        JsonPath pathexpr = JsonPath.compile(jsonPath)
        w << JsonPath.parse(json).read(pathexpr).toString()
    }

    /**
     * Delete document: cascading deletes of subdocuments are controlled by detail. In cassandra this should delete the entire row and it's relations
     *
     * Synchronous (TODO: asynchronous cascade testapi)
     *
     * @param opctx
     * @param detail
     * @param docUUID
     */
    void delDoc(OperationContext opctx, Detail detail, String docUUID) {
        DeleteOperations.deleteDoc(svcs, opctx, detail, docUUID)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        }
    }

    /**
     * Delete document attribute: cascading deletes of the attribute's subdocuments are controlled by detail. In cassandra this deletes a column key within a row
     *
     * Synchronous (TODO: asynchronous cascade testapi call)
     *
     * @param opctx
     * @param detail
     * @param docUUID
     * @param attr
     */
    void delAttr(OperationContext opctx, Detail detail, String docUUID, String attr) {
        DeleteOperations.deleteAttr(svcs, opctx, detail, docUUID, attr, false)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
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
    String newDocFromMap(OperationContext opctx, Detail detail, Map<String, Object> mapDoc) {
        String newid = CreateOperations.newMap(svcs, opctx, detail, mapDoc, false)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        }
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
    String newDoc(OperationContext opctx, Detail detail, String json) {
        String newid = CreateOperations.newDoc(svcs, opctx, detail, json, false)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        }
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
    String newDoc(OperationContext opctx, Detail detail, Reader json) {
        String newid = CreateOperations.newDoc(svcs, opctx, detail, json, false)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        }
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
    String newDocAsync(OperationContext opctx, Detail detail, Reader json) {
        String newid = CreateOperations.newDoc(svcs, opctx, detail, json, true)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        }
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
    void newDocList(OperationContext opctx, Detail detail, Reader jsonListReader, Writer jsonIDList) {
        CreateOperations.newDocStream(svcs, opctx, detail, jsonListReader, jsonIDList)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations

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
    void newAttr(OperationContext opctx, Detail detail, String docUUID, String attr, String json, boolean paxos) {
        CreateOperations.newAttr(svcs, opctx, detail, docUUID, attr, json, paxos)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
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
    void newAttr(OperationContext opctx, Detail detail, String docUUID, String attr, Reader reader, boolean paxos) {
        CreateOperations.newAttr(svcs, opctx, detail, docUUID, attr, reader, paxos)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
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
    void updateAttrPAXOS(OperationContext opctx, Detail detail, String docUUID, String attr, String json, UUID checkVal) {
        opctx.paxosGatekeeperUpdateID = ["P", docUUID] as String[]
        UpdateOperations.updateAttrPAXOS(svcs, opctx, detail, docUUID, attr, json, checkVal)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
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
    void updateAttr(OperationContext opctx, Detail detail, String docUUID, String attr, String json) {
        UpdateOperations.updateAttr(svcs, opctx, detail, docUUID, attr, json)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
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
    void updateAttrEntry(OperationContext opctx, Detail detail, String docUUID, Map.Entry<String, Object> attr) {
        UpdateOperations.updateAttrEntry(svcs, opctx, detail, docUUID, attr)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
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
    Set<String> updateAttrOverlay(OperationContext opctx, Detail detail, String docUUID, String attr, String json) {
        UpdateOperations.updateAttrOverlay(svcs, opctx, detail, docUUID, attr, json)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
        // TODO: Set<String>? is that the new ids created in the process?
        return [] as Set
    }

    List<Object[]> query(OperationContext opctx, Detail detail, String cql, Object[] args) {
        RetrievalOperations.query(svcs, opctx, detail, cql, args)
    }

    String getDocMetadata(OperationContext opctx, Detail detail, String docUUID) {
        Writer writer = new StringWriter()
        String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
        return writer.toString()
    }

    void getDocMetadata(OperationContext opctx, Detail detail, String docUUID, Writer writer) {
        String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    }

    Map<String, Object> deserializeDocMetadata(OperationContext opctx, Detail detail, String docUUID) {
        String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
        Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, metaid, true)
        return doc
    }


    String getAttrMetadata(OperationContext opctx, Detail detail, String docUUID, String attr) {
        Writer writer = new StringWriter()
        String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
        return writer.toString()
    }

    void getAttrMetadata(OperationContext opctx, Detail detail, String docUUID, String attr, Writer writer) {
        String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    }

    Map<String, Object> deserializeAttrMetadata(OperationContext opctx, Detail detail, String docUUID, String attr) {
        String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
        Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, metaid, true)
        return doc
    }


    String getRelMetadata(OperationContext opctx, Detail detail, RelKey rel) {
        Writer writer = new StringWriter()
        String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
        return writer.toString()
    }

    void getRelMetadata(OperationContext opctx, Detail detail, RelKey rel, Writer writer) {
        String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
        RetrievalOperations.getSingleDoc(svcs, opctx, detail, metaid, writer, true)
    }

    Map<String, Object> deserializeRelMetadata(OperationContext opctx, Detail detail, RelKey rel) {
        String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
        Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, metaid, true)
        return doc
    }


    String docMetadataUUID(OperationContext opctx, Detail detail, String docUUID) {
        String metaid = RetrievalOperations.getDocMetadataUUID(svcs, opctx, detail, docUUID)
        if (metaid != null) {
            return metaid
        }
        // create new UUID for META
        Detail initDetail = new Detail()
        initDetail.writeConsistency = detail.readConsistency
        initDetail.docMetaIDMeta = true
        metaid = CreateOperations.newDoc(svcs, opctx, initDetail, '{"_id":"META"}', false)
        UpdDocMetadata upd = new UpdDocMetadata(docUUID: docUUID, metadataUUID: metaid)
        upd.execMutationCassandra(svcs, opctx, initDetail)
        return metaid
    }


    String attrMetadataUUID(OperationContext opctx, Detail detail, String docUUID, String attr) {
        String metaid = RetrievalOperations.getAttrMetadataUUID(svcs, opctx, detail, docUUID, attr)
        if (metaid != null) {
            return metaid
        }
        // create new UUID for META
        metaid = CreateOperations.newDoc(svcs, opctx, detail, '{"_id":"META"}', false)
        UpdAttrMetadata upd = new UpdAttrMetadata(docUUID: docUUID, attr: attr, metadataUUID: metaid)
        upd.execMutationCassandra(svcs, opctx, detail)
        return metaid
    }

    String relMetadataUUID(OperationContext opctx, Detail detail, RelKey rel) {
        String metaid = RetrievalOperations.getRelMetadataUUID(svcs, opctx, detail, rel)
        if (metaid != null) {
            return metaid
        }
        // create doc + relation
        metaid = CreateOperations.newDoc(svcs, opctx, detail, '{"_id":"META"}', false)
        // Update the z_md field in the e table for the doc TODO: should be paxos...
        UpdRelMetadata upd = new UpdRelMetadata(relkey: rel, metadataUUID: metaid)
        upd.execMutationCassandra(svcs, opctx, detail)
        return metaid
    }

    Rel deserializeRel(OperationContext opctx, Detail detail, RelKey relkey) {
        Rel rel = RetrievalOperations.getRel(svcs, opctx, detail, relkey)
        return rel
    }

    /**
     * this is executed as an upsert, so this can be used to update the few non-key fields of a Rel as well
     *
     * @param opctx
     * @param detail
     * @param rel
     */
    void addRel(OperationContext opctx, Detail detail, Rel rel) {
        CreateOperations.addRel(svcs, opctx, detail, rel)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
    }

    void deleteRel(OperationContext opctx, Detail detail, RelKey rel) {
        DeleteOperations.delRel(svcs, opctx, detail, rel)
        if (opctx.executionMode == 'batch') {
            opctx.DO(svcs, detail)
        } // TODO: figure out this vs streaming data operations
    }

    List<Rel> deserializeDocRels(OperationContext opctx, Detail detail, String docUUID) {
        RetrievalOperations.deserializeDocRels(svcs, opctx, detail, docUUID)
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
    Iterator<Map> searchIndex(OperationContext opctx, Detail detail, String indexName, List searchCriteria, List<SearchFilter> filters) {
        Index idx = svcs.collections[opctx.space].second.getIndex(indexName)
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
    void searchIndex(OperationContext opctx, Detail detail, String indexName, List searchCriteria, List<SearchFilter> filters, Writer searchResultsWriter) {
        Iterator<Map> iterator = searchIndex(opctx, detail, indexName, searchCriteria, filters)
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



package cassdoc

import cassdoc.commands.retrieve.QueryToListOfStrArr
import cassdoc.config.CassDocConfig
import cwdrg.lg.annotation.Log
import cwdrg.util.json.JSONUtil
import groovy.transform.CompileStatic

import org.springframework.beans.factory.annotation.Autowired

import cassdoc.commands.mutate.NewAttr
import cassdoc.commands.mutate.UpdFixedCol

import com.fasterxml.jackson.core.JsonFactory

import drv.cassdriver.DriverWrapper
import org.springframework.stereotype.Component

@CompileStatic
@Component
@Log
class CommandExecServices {

    String idField = "_id"

    JsonFactory jsonFactory = new JsonFactory()

    Map<String, Tuple2<TypeConfigurationService, IndexConfigurationService>> collections = [:]

    @Autowired
    CassDocConfig config

    @Autowired
    DriverWrapper driver

    // ---- schema admin

    void loadSystemSchema() {
        OperationContext opctx = new OperationContext(space: 'cassdoc_system_schema')
        Detail dtl = new Detail()
        if (collections == null) {
            collections = [:]
        }
        List<Object[]> schemas = query(opctx, dtl, 'SELECT token(ks), ks, nm, json FROM cassdoc_system_schema.types')
        String curKS
        List<DocType> curList
        for (Object[] collectionSchema : schemas) {
            // todo: nullchecks
            String ks = collectionSchema[1]
            if (!collections.containsKey(ks)) {
                collections[ks] = new Tuple2<>(new TypeConfigurationService(), new IndexConfigurationService())
            }
            if (ks != curKS) {
                if (curList != null) {
                    TypeConfigurationService types = collections[curKS].first
                    types.setTypeList(curList)
                }
                curKS = ks
                curList = []
            }
            String nm = collectionSchema[2]
            String json = collectionSchema[3]
            log.inf("$ks : $nm : $json", null)
            curList << (DocType) JSONUtil.deserialize(json, DocType)
        }
        TypeConfigurationService types = collections[curKS].first
        types.setTypeList(curList)

        // TODO: indexes + index service

    }

    void createSystemSchema() {
        log.wrn('CASSDOC BASE SCHEMA CREATE', null, null)
        // create base schema
        driver.executeDirectUpdate('cassdoc_system_schema', CassandraSchemaUtil.createSchemaKeyspace(1), null, 'QUORUM', null)
        driver.executeDirectUpdate('cassdoc_system_schema', CassandraSchemaUtil.createSchemaTypesTable(), null, 'QUORUM', null)
        driver.executeDirectUpdate('cassdoc_system_schema', CassandraSchemaUtil.createSchemaIndexesTable(), null, 'QUORUM', null)
        log.inf('... Base schema created', null, null)
    }

    void createNewCollectionSchema(String collectionName) {
        log.inf("CASSDOC COLLECTION SCHEMA CREATE $collectionName", null)
        driver.executeDirectUpdate('cassdoc_system_schema', CassandraSchemaUtil.createKeyspace(collectionName), null, 'QUORUM', null)
        driver.executeDirectUpdate(collectionName, CassandraSchemaUtil.createRelationTable(collectionName), null, 'QUORUM', null)
        driver.executeDirectUpdate(collectionName, CassandraSchemaUtil.createIndexTable(collectionName), null, 'QUORUM', null)
        driver.executeDirectUpdate(collectionName, CassandraSchemaUtil.createAttrTable(collectionName, 'META'), null, 'QUORUM', null)
        driver.executeDirectUpdate(collectionName, CassandraSchemaUtil.createEntityTable(collectionName, 'META', []), null, 'QUORUM', null)
        driver.executeDirectUpdate(
                collectionName,
                CassandraSchemaUtil.insertSchemaType(),
                [collectionName, 'META', JSONUtil.toJSON(new DocType(uri: 'cassdoc.meta', suffix: 'META'))] as Object[],
                'QUORUM',
                null)
        TypeConfigurationService typesvc = new TypeConfigurationService(typeList: [new DocType(uri: 'cassdoc.meta', suffix: 'META')])
        collections[collectionName] = new Tuple2<>(typesvc, new IndexConfigurationService())
        log.inf("... done with CASSDOC COLLECTION SCHEMA CREATE $collectionName", null)
    }

    void createNewDoctypeSchema(String collectionName, DocType type) {
        String typeCode = type?.suffix
        log.inf("CASSDOC DOCTYPE SCHEMA CREATE $typeCode in collection $collectionName", null)
        driver.executeDirectUpdate(collectionName, CassandraSchemaUtil.createAttrTable(collectionName, typeCode), null, 'QUORUM', null)
        driver.executeDirectUpdate(collectionName, CassandraSchemaUtil.createEntityTable(collectionName, typeCode, type.fixedAttrList), null, 'QUORUM', null)
        driver.executeDirectUpdate(
                collectionName,
                CassandraSchemaUtil.insertSchemaType(),
                [collectionName, typeCode, JSONUtil.toJSON(type)] as Object[],
                'QUORUM',
                null)
        if (collections != null) {
            collections[collectionName] = new Tuple2<>(
                    new TypeConfigurationService(typeList: collections[collectionName].first.typeList + new DocType(uri: 'cassdoc.' + typeCode, suffix: typeCode)),
                    collections[collectionName].second)
        }
        log.inf("... done with CASSDOC DOCTYPE SCHEMA CREATE $typeCode in collection $collectionName", null)
    }

    // revisit: https://stackoverflow.com/questions/8297705/how-to-implement-thread-safe-lazy-initialization
    // ... and of course, holy shit, multiple cassdoc servers ... I'll probably need zookeeper or something similar
    // ... ... or https://www.datastax.com/dev/blog/consensus-on-cassandra
    // ... ... or batch the execution
    void checkSchema(String collectionName) {
        if (collections == null) {
            synchronized (collections) {
                if (collections == null) {
                    // check the base schema
                    if (!driver.keyspaces.contains('cassdoc_system_schema')) {
                        if (config?.autoCreateBaseSchema) {
                            createSystemSchema()
                        } else {
                            throw log.err('CASSDOC BASE SCHEMA / cassdoc_system_schema not found and autoinitialize disabled', new IllegalStateException('cassdoc_system_schema not found'), null)
                        }
                    }
                    collections = [:]
                }
                loadSystemSchema()
            }
        }
        if (!collections.containsKey(collectionName)) {
            if (config.autoCreateNewKeyspaces) {
                createNewCollectionSchema(collectionName)
            } else {
                throw log.err('', new IllegalArgumentException("Unknown cassdoc collection $collectionName"))
            }
        }
    }

    void checkDocType(String collectionName, String typeCode) {
        DocType type = collections[collectionName].first.getTypeForSuffix(typeCode)
        if (type == null) {
            if (config.autoCreateNewDocTypes) {
                createNewDoctypeSchema(collectionName, new DocType(uri: 'cassdoc.'+typeCode, suffix: typeCode))
            } else {
                throw log.err('', new IllegalArgumentException("Unknown cassdoc doc type $typeCode for $collectionName"))
            }
        }
    }

    // ---- utility....

    List<Object[]> query(OperationContext opctx, Detail detail, String cql, Object[] args) {
        QueryToListOfStrArr cmd = new QueryToListOfStrArr(query: cql)
        if (args != null)
            cmd.initiateQuery(this, opctx, detail, args)
        else
            cmd.initiateQuery(this, opctx, detail)
        List queryresult = []
        Object[] data = null
        while (true) {
            data = cmd.nextRow()
            if (data == null) break
            queryresult.add(data)
        }
        return queryresult
    }

    // ---- COMMANDS (need to move to appropriate locaiton)

    static void execUpdDocFixedColUNSAFE(CommandExecServices svcs, OperationContext opctx, Detail detail, UpdFixedCol updDocFixedColCmd) {
        String space = opctx.space
        String suffix = IDUtil.idSuffix(updDocFixedColCmd.docUUID)
        String colName = updDocFixedColCmd.colName
        String cql = "UPDATE ${space}.e_${suffix} SET ${colName} = ? WHERE e = ?"
        Object[] args = [
                updDocFixedColCmd.value,
                updDocFixedColCmd.docUUID] as Object[]
        svcs.driver.executeDirectUpdate(space, cql, args, detail.writeConsistency, opctx.operationTimestamp)
    }

    static void execNewAttrCmdPAXOS(CommandExecServices svcs, OperationContext opctx, Detail detail, NewAttr newAttrCmd) {
        String space = opctx.space
        String suffix = IDUtil.idSuffix(newAttrCmd.docUUID)
        String cql = "INSERT INTO ${space}.p_${suffix} (e,p,zv,d,t) VALUES (?,?,?,?,?) IF NOT EXISTS"
        Object[] args = [
                newAttrCmd.docUUID,
                newAttrCmd.attrName,
                opctx.updateUUID,
                newAttrCmd.attrValue?.value,
                TypeConfigurationService.attrTypeCode(newAttrCmd.attrValue?.type)] as Object[]
        svcs.driver.executeDirectUpdate(space, cql, args, detail.writeConsistency, opctx.operationTimestamp)
    }

}

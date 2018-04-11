package cassdoc.testapi

import cassdoc.CassdocAPI
import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.inittest.JavaApiTestInitializer
import cassdoc.tests.Utils
import cwdrg.util.json.JSONUtil
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import spock.lang.Shared
import spock.lang.Specification

import static org.junit.Assert.assertTrue
import static org.junit.Assert.assertTrue

class JavaApiIntegrationSpec extends Specification {

    static String keyspace = 'java_api_test'

    @Shared
    CassdocAPI api

    OperationContext opctx = new OperationContext(space: keyspace)
    Detail detail = new Detail()

    void setupSpec() {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra()
        api = JavaApiTestInitializer.initAPI()
        //JavaApiTestInitializer.initCassandraSchema(keyspace, api.svcs.driver, api.svcs.typeSvc)
        api.svcs.createSystemSchema()
        api.svcs.createNewCollectionSchema(keyspace)
        api.svcs.createNewDoctypeSchema(api.svcs.collections[keyspace].first.getTypeForSuffix('PROD'))
        api.svcs.createNewDoctypeSchema(api.svcs.collections[keyspace].first.getTypeForSuffix('JOB'))
    }

    void 'check cassandra status'() {
        when:
        Set<String> ks = api.svcs.driver.keyspaces

        then:
        ks.toString().contains(keyspace)
    }

    void 'persist simple json document and perform some basic attr manipulations'() {
        given:
        String doc = this.class.classLoader.getResourceAsStream('cassdoc/testdata/SimpleDoc.json').getText()

        when: 'we persist a single simple doc and then retrieve it'
        Map original = JSONUtil.deserializeMap(doc)
        String newid = api.newDoc(opctx, detail, doc)
        // need to "match" the id key in the original doc so we can do compares
        original._id = newid
        String json = api.getDoc(opctx, detail, newid)
        Map retrieved = JSONUtil.deserializeMap(json)
        Map retrievedMap = api.deserializeDoc(opctx, detail, newid)

        then: 'that doc matches the original'
        original == retrieved
        original == retrievedMap

        when: 'we then set some new attributes on the doc'
        api.delAttr(opctx, detail, newid, "hello", false)
        api.newAttr(opctx, detail, newid, 'newInt', '22', false)
        api.newAttr(opctx, detail, newid, 'newString', '"str"', false)
        api.newAttr(opctx, detail, newid, 'newNull', 'null', false)
        api.newAttr(opctx, detail, newid, 'newFloat', '8.999933', false)
        api.newAttr(opctx, detail, newid, 'newArray', '[5,10,15,"20"]', false)
        api.newAttr(opctx, detail, newid, 'newComplex', '{"a":"aaaa","b":22,"c":null}', false)
        original.remove('hello')
        original.put('newInt', 22)
        original.put('newNull', null)
        original.put('newString', 'str')
        original.put('newFloat', 8.999933)
        original.put('newArray', [5, 10, 15, '20'])
        original.put('newComplex', ['a': 'aaaa', 'b': 22, 'c': null])
        json = api.getDoc(opctx, detail, newid)
        retrieved = JSONUtil.deserializeMap(json)
        retrievedMap = api.deserializeDoc(opctx, detail, newid)

        then: 're-retrieval still matches the original and respCode methods of getting these simple attrs works'
        original == retrieved
        original == retrievedMap
        // potentially recursive attribute retrieval (won't recurse here though)
        api.getAttr(opctx, detail, newid, 'newInt') == '22'
        api.getAttr(opctx, detail, newid, 'newString') == '"str"'
        api.getAttr(opctx, detail, newid, 'newNull') == 'null'
        api.getAttr(opctx, detail, newid, 'newFloat') == '8.999933'
        api.getAttr(opctx, detail, newid, 'newArray') == '[5,10,15,"20"]'
        api.getAttr(opctx, detail, newid, 'newComplex') == '{"a":"aaaa","b":22,"c":null}'
        // never recursive retrieval
        api.getSimpleAttr(opctx, detail, newid, 'newInt') == '22'
        api.getSimpleAttr(opctx, detail, newid, 'newString') == '"str"'
        api.getSimpleAttr(opctx, detail, newid, 'newNull') == 'null'
        api.getSimpleAttr(opctx, detail, newid, 'newFloat') == '8.999933'
        api.getSimpleAttr(opctx, detail, newid, 'newArray') == '[5,10,15,"20"]'
        api.getSimpleAttr(opctx, detail, newid, 'newComplex') == '{"a":"aaaa","b":22,"c":null}'

        when: 'we do some overwrites to attrs'
        api.updateAttr(opctx, detail, newid, 'newInt', '45')

        then:
        api.getAttr(opctx, detail, newid, 'newInt') == '45'
    }

    void 'test doc persist with child documents'() {
        given: 'we setup detail to pull the child docs'
        detail.pullChildDocs = true
        String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/DocWithSubDocs.json").getText()

        when: 'we create a document with child documents and do some jsonpath expressions too'
        String newid = api.newDoc(opctx, detail, doc)
        String json = api.getDoc(opctx, detail, newid)
        Map original = JSONUtil.deserializeMap(doc)
        Map retrieved = JSONUtil.deserializeMap(json)
        Map retrMap = api.deserializeDoc(opctx, detail, newid)
        // since _id's are changed/created as the docs are, for comparison purposes we strip the _id fields
        Utils.stripKeys(original, '_id')
        Utils.stripKeys(retrieved, '_id')
        Utils.stripKeys(retrMap, '_id')
        // calculate a path and attr jsonpath expression
        String pathexpr = '$.arrVal[1]'
        String result = api.getDocJsonPath(opctx, detail, newid, pathexpr)
        String result2 = api.getAttrJsonPath(opctx, detail, newid, "arrVal", '$[2]')

        then: 'original matches retrieved (stripped of ids)'
        original == retrieved
        original == retrMap
        result == 'matey'
        result2 == 'arrrr'
    }

    void 'test some detail features'() {
        given:
        String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/DocWithSubDocs.json").getText()
        String newid = api.newDoc(opctx, detail, doc)
        Map original = JSONUtil.deserializeMap(doc)
        original._id = newid

        when: 'we use a detail to get a single doc that specifies a subset of attributes'
        Detail subsetDTL = new Detail()
        subsetDTL.pullChildDocs = true
        subsetDTL.attrSubset = ['nullVal', 'hello', 'iVal'] as Set
        String json = api.getDoc(opctx, subsetDTL, newid)
        Map subsetOrig = [:]
        subsetOrig._id = newid
        subsetOrig.nullVal = original.nullVal
        subsetOrig.hello = original.hello
        subsetOrig.iVal = original.iVal

        then:
        subsetOrig == JSONUtil.deserializeMap(json)

        when: 'we use a detail to get a single doc the specifies a set of attributes to exclude'
        Detail exclDTL = new Detail()
        exclDTL.pullChildDocs = true
        exclDTL.attrExclude = [
                '$%^&*@#(&*#$(){}[]--09*(*()62347813420<>,.,<~`/?',
                'JustASubDoc',
                'hello',
                '&^%$HeteroArrayOfSubdocs',
                'fVal'] as Set
        json = api.getDoc(opctx, exclDTL, newid)
        Map excl = JSONUtil.deserializeMap(doc)
        excl._id = newid
        excl.remove('$%^&*@#(&*#$(){}[]--09*(*()62347813420<>,.,<~`/?')
        excl.remove('JustASubDoc')
        excl.remove('hello')
        excl.remove('&^%$HeteroArrayOfSubdocs')
        excl.remove('fVal')

        then:
        excl == JSONUtil.deserializeMap(json)

        when:
        exclDTL.attrExclude = [
                'SingleSubDoc',
                'fVal'] as Set
        json = api.getDoc(opctx, exclDTL, newid)
        Map excl2 = JSONUtil.deserializeMap(json)

        then:
        !excl2.JustASubDoc.containsKey('SingleSubDoc')

        when: 'a detail where special detail is applied to a single attribute (JustASubDoc)'
        // such that only that attribute's subdocs are pulled
        Detail attrSpecialDTL = new Detail()
        Detail attrsubDTL = new Detail()
        attrsubDTL.pullChildDocs = true
        attrSpecialDTL.attrDetail = [:]
        attrSpecialDTL.attrDetail.JustASubDoc = attrsubDTL
        json = api.getDoc(opctx, attrSpecialDTL, newid)
        Map spcl = JSONUtil.deserializeMap(json)

        then:
        !spcl['&^%$HeteroArrayOfSubdocs'][0].containsKey('firstDoc')
        spcl.JustASubDoc.SingleSubDoc == 'yeppers'

        when: 'detail that only applies to subdocuments of a specific type/collection'
        Detail typeSpecificDTL = new Detail()
        typeSpecificDTL.pullChildDocs = true
        typeSpecificDTL.childDocSuffixDetail = [:]
        typeSpecificDTL.childDocSuffixDetail["PROD"] = new Detail()
        typeSpecificDTL.childDocSuffixDetail.PROD.docIDTimestampMeta = true
        json = api.getDoc(opctx, typeSpecificDTL, newid)
        Map typSpec = JSONUtil.deserializeMap(json)

        then:
        typSpec != null
    }

    void 'test fixed attributes'() {
        when: 'we set a fixed attribute parameter on a doc and direct-query the entity table for that value'
        String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/DocWithFixedAttrs.json").getText()
        String newid = api.newDoc(opctx, detail, doc)
        Map original = JSONUtil.deserializeMap(doc)
        List checkGtin = api.query(opctx, detail, "SELECT token(e),gtin FROM ${keyspace}.e_prod where e = '$newid'")

        then: 'the returned value for GTIN matches the attribute value we set'
        assertTrue(original["dbpedia:GTIN"] == checkGtin[0][1])

        when: 'we query for the fixed submission date'
        checkGtin = api.query(opctx, detail, "SELECT token(e),submit_date FROM proto_jsonstore.e_prod where e = '$newid'")

        then: 'the submission dates match'
        assertTrue(original["product:submitdate"] == Long.parseLong(checkGtin[0][1]))

        // TODO: more column datatypes
    }

    void 'test metadata relations are set for a subdocument'()
    {
        when: 'we persist json with a known type subdocument'
        String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/DocWithOneSubDoc.json").getText()
        String newid = api.newDoc(opctx, detail, doc)
        Map original = JSONUtil.deserializeMap(doc)
        original._id = newid

        String docmeta = api.docMetadataUUID(opctx, detail, newid)
        String attrmeta = api.attrMetadataUUID(opctx, detail, newid, "fVal")
        String docmeta2 = api.docMetadataUUID(opctx, detail, newid)
        String attrmeta2 = api.attrMetadataUUID(opctx, detail, newid, "fVal")

        then:
        // what are we testing here???
        docmeta == docmeta2
        attrmeta == attrmeta2

        when:
        List<Rel> rels = api.deserializeDocRels(opctx, detail, newid)
        String relmeta = api.relMetadataUUID(opctx, detail, rels[0].relKey)
        String relmeta2 = api.relMetadataUUID(opctx, detail, rels[0].relKey)

        then:
        // what are we testing here???
        relmeta == relmeta2
    }

    void cleanupSpec() {
        //EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
    }
}

// unneeded annotations (did not work, PITA, or didn't encounter the same errors), but for reference:
//@CassandraDataSet(value = 'cql/setup.cql', keyspace = 'integration_test')
//@EmbeddedCassandra //configuration = 'cu-cassandra.yaml", clusterName = 'Test Cluster", host = '127.0.0.1", port = 9142)
// may need: https://stackoverflow.com/questions/33840156/how-can-i-start-an-embedded-cassandra-server-before-loading-the-spring-context
//@TestExecutionListeners(listeners = [ CassandraUnitDependencyInjectionTestExecutionListener, CassandraUnitTestExecutionListener, DependencyInjectionTestExecutionListener.class ])

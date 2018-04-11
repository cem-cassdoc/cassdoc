package cassdoc.tests;

import static org.junit.Assert.*

import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import cassdoc.CassdocAPI
import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.inittest.JavaApiTestInitializer
import cwdrg.util.json.JSONUtil

/**
 * Disclaimer: these tests are executed against a single-node local cassandra. As such, many 
 * issues with eventual consistency will not be evident here. This set of tests is for checking
 * the essential operational logic works.
 * 
 * Where this would become problematic / race condition vulnerable is in the numerous instances 
 * where we create something and then immediately begin pulling it. Granted, our detail levels 
 * are defaulting to LOCAL_QUORUM so that should protect us ...
 * 
 * 
 * @author cowardlydragon
 *
 */

class BasicTests {
  static CassdocAPI api

  @BeforeClass
  static void setup() {
    println "---- BEGIN"
    api = JavaApiTestInitializer.initAPI()
  }

  /** 
   * test that the metadata get/init calls work. 
   * 
   * Once the metadata id is initialized, normal attribute get/set is done to manipulate the metadata fields/attributes.
   * 
   * I'm not sure I like these "side effect" testapi calls that look like gets, but I took "get" out to be sure...
   * 
   */
  @Test
  public void testMetadataInit() {
    OperationContext opctx = new OperationContext()
    opctx.space = "proto_jsonstore"
    Detail detail = new Detail()
    String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/DocWithOneSubDoc.json").getText()
    String newid = api.newDoc(opctx, detail, doc)
    Map original = JSONUtil.deserializeMap(doc)
    original._id = newid

    String docmeta = api.docMetadataUUID(opctx, detail, newid)
    String attrmeta = api.attrMetadataUUID(opctx, detail, newid, "fVal")
    String docmeta2 = api.docMetadataUUID(opctx, detail, newid)
    String attrmeta2 = api.attrMetadataUUID(opctx, detail, newid, "fVal")
    println "metaids: $docmeta $docmeta2 $attrmeta $attrmeta2"
    assertTrue(docmeta == docmeta2)
    assertTrue(attrmeta == attrmeta2)

    List<Rel> rels = api.deserializeDocRels(opctx, detail, newid)
    println "rels:" +JSONUtil.serialize(rels)
    String relmeta = api.relMetadataUUID(opctx, detail, rels[0].relKey)
    String relmeta2 = api.relMetadataUUID(opctx, detail, rels[0].relKey)
    assertTrue(relmeta == relmeta2)
  }

  @Test
  void testHasValueManualIndex() {

    OperationContext opctx = new OperationContext(space:"proto_jsonstore")
    Detail detail = new Detail()

    // doc with an attribute that triggers a manual index
    String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/DocWithIndexTrigger.json").getText()
    String newid = api.newDoc(opctx, detail, doc)

    println "idx trigger: "+newid

    List index = api.query(opctx, detail, "SELECT token(i1,i2,i3,k1,k2,k3),v1 from ${opctx.space}.i WHERE i1 = 'PROD' and i2 = 'IC' and i3 = 'HV' and k1 = 'AZ889__55' and k2 = '' and k3 = ''")

    assertTrue(index.find{it[1] == newid} != null)

    // deletes that have the same timestamp as the update timestamp of a cell are ignored. delete timestamp must be > update timestamp.
    // I *think* subsequent updates to a cell with the same timestamp that come in do overwrite, as in "last one wins"
    // huh, why didn't the basic tests fail??? They reuse the same opctx...
    // in that instance, we do a delete, then a select, maybe that asserts the delete.
    // maybe the groovy map compare operation isn't accurate...
    // maybe it is the complexity of the i table's keys, or the trailing ""'s in the v2/v3 fields, or that d/id are null, whereas the other delete of an attribute had cells in the cluster key
    println "NEW OPCTX"
    opctx = new OperationContext(space:"proto_jsonstore")
    api.delAttr(opctx, detail, newid, "proprietary:InternalCode", false)

    index = api.query(opctx, detail, "SELECT token(i1,i2,i3,k1,k2,k3),v1 from ${opctx.space}.i WHERE i1 = 'PROD' and i2 = 'IC' and i3 = 'HV' and k1 = 'AZ889__55' and k2 = '' and k3 = ''")
    boolean found = (index.find{it[1] == newid} != null)
    println "FoundAPI: "+found

    // code used to debug the not-deleteing deleting (because we didn't use a new OpCtx)
    //    String manprep = "DELETE FROM proto_jsonstore.i WHERE i1 = ? and i2 = ? and i3 = ? and k1 = ? and k2 = ? and k3 = ? and v1 = ? and v2 = ? and v3 = ?"
    //    Object[] args =  [
    //      'PROD',
    //      'IC',
    //      'HV',
    //      'AZ889__55',
    //      '',
    //      '',
    //      newid,
    //      '',
    //      ''] as Object[]
    //    testapi.svcs.driver.executeDirectUpdate("proto_jsonstore", manprep, args, "LOCAL_QUORUM", null)
    //
    //    index = testapi.query(opctx, detail, "SELECT token(i1,i2,i3,k1,k2,k3),v1 from ${opctx.space}.i WHERE i1 = 'PROD' and i2 = 'IC' and i3 = 'HV' and k1 = 'AZ889__55' and k2 = '' and k3 = ''")
    //
    //    boolean found2 = (index.find{it[1] == newid} != null)
    //    println "FoundManPrep: "+found2
    //
    //
    //    String man = "DELETE FROM proto_jsonstore.i WHERE i1 = 'PROD' and i2 = 'IC' and i3 = 'HV' and k1 = 'AZ889__55' and k2 = '' and k3 = '' and v1 = '$newid' and v2 = '' and v3 = ''"
    //    testapi.svcs.driver.executeDirectUpdate("proto_jsonstore", man, null, "LOCAL_QUORUM", null)
    //    index = testapi.query(opctx, detail, "SELECT token(i1,i2,i3,k1,k2,k3),v1 from ${opctx.space}.i WHERE i1 = 'PROD' and i2 = 'IC' and i3 = 'HV' and k1 = 'AZ889__55' and k2 = '' and k3 = ''")
    //
    //    boolean found3 = (index.find{it[1] == newid} != null)
    //    println "FoundMan: "+found3


    assertTrue(found == false)

  }

  // TODO: detail level

  // TODO: conditional update / paxos

  // TODO: batch mode processing, sync vs async

  // TODO: index tests

  @AfterClass
  static void teardown() {
    api.svcs.driver.destroy()
    println "---- END"

  }
}

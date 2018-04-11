package cassdoc.tests

import cassdoc.CassdocAPI
import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.inittest.JavaApiTestInitializer
import cassdoc.operations.SearchOperations
import cwdrg.util.json.JSONUtil
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

class ThreadedGetIdListTest {
  static CassdocAPI api


  @BeforeClass
  static void setup() {
    println "---- BEGIN"
    api = JavaApiTestInitializer.initAPI()
  }

  @Test
  public void testESDW() {
    String doc = this.class.classLoader.getResourceAsStream("cassdoc/tests/SimpleDoc.json").getText()
    List ids = []
    100.times {
      OperationContext opctx = new OperationContext()
      opctx.space = "proto_jsonstore"
      Detail detail = new Detail()
      String newid = api.newDoc(opctx, detail, doc)
      ids.add(newid)
    }

    println ("DOC CREATION DONE")
    StringWriter outwrt = new StringWriter()
    SearchOperations.retrieveIDListThreadPoolExecutor(api.svcs, new OperationContext(space:"proto_jsonstore"),new Detail(),ids.iterator(),outwrt)
    println ("TEST COMPLETE")
    String json = outwrt.toString()
    println json
    List results = JSONUtil.deserializeList(json)
    println("count: "+results.size())
  }

  @AfterClass
  static void teardown() {
    api.svcs.driver.destroy()
    println "---- END"
  }
}

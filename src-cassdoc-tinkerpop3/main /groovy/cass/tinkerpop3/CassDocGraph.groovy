package cass.tinkerpop3;

import org.apache.commons.configuration.Configuration
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.Graph.Exceptions
import org.apache.tinkerpop.gremlin.structure.Graph.Variables

import cassdoc.API
import cassdoc.CassDocJsonUtil
import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.Rel
import cassdoc.RelKey
import cwdrg.lg.annotation.Log


@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
//@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
//@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
//@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
//@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
//@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
//@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
//@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
//@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
@Log
class CassDocGraph implements Graph {

  String space;
  API    cassDocAPI;

  static boolean testMode = false


  static CassDocGraph open(final Configuration configuration)
  {
    CassDocGraph cassDocGraph = new CassDocGraph()
    if (testMode) {
      println "CASSDOCGRAPH TEST MODE TRIGGERED"
      cassDocGraph.space = CassDocGraphTestHelper.space
      cassDocGraph.cassDocAPI = CassDocGraphTestHelper.api
    }
  }

  @Override
  Vertex addVertex(Object... keyValues) {
    //log.dbg("CassDocGraph: addVertex invoked: NOT SUPPORTED: use CassDoc API methods to add a new document/vertex")
    //throw Graph.Exceptions.vertexAdditionsNotSupported()

    // usual rules: first pair MUST be _id and indicate the type
    if (keyValues == null) throw new RuntimeException("Invalid null/empty keyvalues for new vertex")
    if (keyValues.length == 0) throw new RuntimeException("Invalid zero-length keyvalues for new vertex")
    if (keyValues.length %2 != 0) throw new RuntimeException("Unbalanced number of varargs")
    if (keyValues[0] != "_id") throw new RuntimeException("_id type indicator required as first property pair")

    OperationContext opctx = new OperationContext(space:space)
    Detail detail = new Detail()

    String docid = cassDocAPI.newDoc(opctx,detail,"""{"_id":"${keyValues[1]}"}""")
    for (int i=1; i < keyValues.length / 2; i++) {
      String attrName = keyValues[i*2]
      Object attrVal = keyValues[i*2+1]
      StringWriter w = new StringWriter()
      // todo: newDoc/newAttr/update that don't have serialization overhead
      CassDocJsonUtil.specialSerialize(attrVal,w)
      cassDocAPI.newAttr(opctx, detail, docid, attrName, w.toString())
    }

    CassDocVertex v = new CassDocVertex(docId:docid,cassDocGraph:this)
    return v
  }


  @Override
  void close() throws Exception {
    // close driver???
    // ignore since it is read only?
  }


  /**
   * Neo4j doesn't implement this...
   * 
   * http://tinkerpop.apache.org/docs/3.0.1-incubating/#graphcomputer
   */  
  @Override
  GraphComputer compute() throws IllegalArgumentException {
    throw Exceptions.graphComputerNotSupported()
  }


  @Override
  <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
    throw Exceptions.graphComputerNotSupported()
  }


  @Override
  Configuration configuration() {
    // ??? get types ??? keyspaces ???
    return null;
  }


  @Override
  Iterator<Edge> edges(Object... edgeIds) {
    OperationContext opctx = new OperationContext(space:space)
    Detail detail = new Detail()
    List edges = []
    // TODO: spawn thread for streaming object iterator rather than do full list construction
    for (Object o : edgeIds) {
      Rel rel = cassDocAPI.deserializeRel(opctx, detail, (RelKey)o)
      CassDocEdge edge = new CassDocEdge(rel:rel,cassDocGraph:this)
      edges.add(edge)
    }
    return edges.iterator()
  }


  @Override
  Transaction tx() {
    throw Exceptions.transactionsNotSupported();
  }


  @Override
  Variables variables() {
    // Graph keyspace???
    return null;
  }


  @Override
  Iterator<Vertex> vertices(Object... vertexIds) {
    // vertexID: space,id tuple
    log.dbg("CassDoc: get vertices for Ids "+vertexIds,null,null)
    OperationContext opctx = new OperationContext(space:space)
    Detail detail = new Detail()
    List<Vertex> vertices = []
    // TODO: spawn thread for streaming object iterator rather than do full list construction
    for (Object id : vertexIds) {
      log.dbg("deserialize "+id)
      Map<String,Object> doc = cassDocAPI.deserializeDoc(null, null, (String)id)
      CassDocVertex vertex = new CassDocVertex(docId:id,cassDocGraph:this)
      vertices.add(vertex)
    }
    return vertices.iterator()
  }
}


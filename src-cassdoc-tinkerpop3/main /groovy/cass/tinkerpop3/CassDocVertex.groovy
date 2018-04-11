package cass.tinkerpop3

import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringUtils
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

import cassdoc.CassDocJsonUtil
import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.Rel


@CompileStatic
class CassDocVertex implements Vertex {

  String docId
  transient CassDocGraph cassDocGraph

  @Override
  Graph graph() {
    cassDocGraph
  }

  @Override
  void remove() {
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    cassDocGraph.cassDocAPI.delDoc(opctx, detail, docId, false)
  }

  @Override
  Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    Rel rel = new Rel()
    rel.p1 = docId
    rel.c1 = ((CassDocVertex)inVertex).docId
    rel.ty1 = label
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    cassDocGraph.cassDocAPI.addRel(opctx, detail, rel)
    CassDocEdge edge = new CassDocEdge(rel:rel,cassDocGraph:cassDocGraph)
    if (keyValues != null ) {

      String relMetaId = cassDocGraph.cassDocAPI.relMetadataUUID(opctx, detail, rel.relKey)
      for (int i = 0; i < keyValues.length / 2; i++) {
        String key = keyValues[i*2].toString()
        Object val = keyValues[i*2+1]
        StringWriter w = new StringWriter()
        CassDocJsonUtil.specialSerialize(val,w)
        cassDocGraph.cassDocAPI.newAttr(opctx, detail, relMetaId, key, w.toString(),false, false)
      }
    }
  }

  @Override
  <V> VertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
    throw Element.Exceptions.propertyAdditionNotSupported();
  }

  @Override
  Object id() {
    docId
  }

  @Override
  String label() {
    // is label an id??? no... id() gets you id.
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    Map<String,Object> docMeta = cassDocGraph.cassDocAPI.deserializeDocMetadata(opctx, detail, docId)
    String label = docMeta["label"]

    return label
  }



  @Override
  Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    Set<String> labels = null
    if (edgeLabels != null && edgeLabels.length > 0) {
      labels = [] as Set
      labels.addAll(edgeLabels)
    }
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    List<Rel> docRels = cassDocGraph.cassDocAPI.deserializeDocRels(opctx, detail, docId)
    List<Edge> edgeList = []

    for (Rel rel : docRels) {
      if (labels == null && !StringUtils.startsWith(rel.ty1,"_")) {
        edgeList.add(new CassDocEdge(cassDocGraph:cassDocGraph,rel:rel))
      } else if (!StringUtils.startsWith(rel.ty1,"_") && labels.contains(rel.ty1)) {
        edgeList.add(new CassDocEdge(cassDocGraph:cassDocGraph,rel:rel))
      }
    }
    return edgeList.iterator()
  }


  @Override
   <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()

    if (propertyKeys != null) {
      Set<String> propnames = [] as Set
      propnames.addAll(propertyKeys)
      if (propnames.size() > 0) {
        detail.setAttrSubset(propnames)
      }
    }

    List<Property> props = []
    Map<String,Object> docProps = cassDocGraph.cassDocAPI.deserializeDoc(opctx, detail, docId)

    for (Map.Entry<String,Object> entry : docProps.entrySet()) {
      CassDocProperty prop = new CassDocVertexProperty(cassDocGraph:cassDocGraph,docId:docId,key:entry.key,value:entry.value,vertex:this)
      props.add(prop)
    }

    return props.iterator()
  }


  @Override
  Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    Set<String> labels = null
    if (edgeLabels != null && edgeLabels.length > 0) {
      labels = [] as Set
      labels.addAll(edgeLabels)
    }
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    List<Rel> docRels = cassDocGraph.cassDocAPI.deserializeDocRels(opctx, detail, docId)
    List<Vertex> vertexList = []

    for (Rel rel : docRels) {
      if (labels == null) {
        if (direction.equals(Direction.OUT) && !StringUtils.startsWith(rel.ty1,"_") && !StringUtils.startsWith(rel.ty1,"-")) {
          vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
        } else if (direction.equals(Direction.IN) && !StringUtils.startsWith(rel.ty1,"_")&& StringUtils.startsWith(rel.ty1,"-")) {
          vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
        } else if (direction.equals(Direction.BOTH) && !StringUtils.startsWith(rel.ty1,"_")) {
          vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
        }
      } else {
        if (direction.equals(Direction.OUT) && !StringUtils.startsWith(rel.ty1,"_") && !StringUtils.startsWith(rel.ty1,"-") && labels.contains(rel.ty1)) {
          vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
        } else if (direction.equals(Direction.IN) && !StringUtils.startsWith(rel.ty1,"_") && StringUtils.startsWith(rel.ty1,"-") && labels.contains("-"+rel.ty1)) {
          vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
        } else if (direction.equals(Direction.BOTH)) {
          if (rel.ty1.startsWith("-") && labels.contains(rel.ty1.substring(1))) {
            vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
          } else if (labels.contains(rel.ty1)) {
            vertexList.add(new CassDocVertex(cassDocGraph:cassDocGraph,docId:rel.c1))
          }
        }
      }
    }
    return vertexList.iterator()
  }
}


package cass.tinkerpop3

import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex

import cassdoc.CassDocJsonUtil
import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.Rel

class CassDocEdge implements Edge {
  Rel rel
  transient CassDocGraph cassDocGraph

  @Override
  <V> Property<V> property(String key, V value) {
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    String relMetaId = cassDocGraph.cassDocAPI.relMetadataUUID(opctx, detail, rel.relKey)
    StringWriter w = new StringWriter()
    CassDocJsonUtil.specialSerialize(value,w)
    cassDocGraph.cassDocAPI.newAttr(opctx, detail, relMetaId, key, w.toString())
    CassDocEdgeProperty prop = new CassDocEdgeProperty(docId:relMetaId,rel:rel,cassDocGraph:cassDocGraph,key:key,value:value)
    return prop
  }

  @Override
  void remove() {
    // TODO: cassdoc API needs Rel cleanup
    throw Edge.Exceptions.edgeRemovalNotSupported();
  }


  @Override
  Graph graph() {
    cassDocGraph
  }

  @Override
  Object id() {
    rel.relKey
  }

  @Override
  String label() {
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    Map<String,Object> relMeta = cassDocGraph.cassDocAPI.deserializeRelMetadata(opctx, detail, rel.relKey)
    String label = relMeta["label"]
    return label
  }


  @Override
  <V> Iterator<Property<V>> properties(String... propertyKeys) {
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
    Map<String,Object> relMeta = cassDocGraph.cassDocAPI.deserializeRelMetadata(opctx, detail, rel.relKey)
    String metadataId = relMeta["_id"]

    for (Map.Entry<String,Object> entry : relMeta.entrySet()) {
      CassDocProperty prop = new CassDocEdgeProperty(cassDocGraph:cassDocGraph,docId:metadataId,edge:this)
      props.add(prop)
    }

    return props.iterator()
  }

  @Override
  Iterator<Vertex> vertices(Direction direction) {
    // get necessary Rels based on direction / label
    switch (direction) {
      case Direction.OUT:
      // return the child
        List<Vertex> childvertex = [
          new CassDocVertex(docId:rel.c1,cassDocGraph:cassDocGraph)
        ]
        return childvertex.iterator()
      case Direction.IN:
        List<Vertex> parentvertex = [
          new CassDocVertex(docId:rel.p1,cassDocGraph:cassDocGraph)
        ]
        return parentvertex.iterator()
      default:
      // return both child and parent
        List<Vertex> both = [
          new CassDocVertex(docId:rel.p1,cassDocGraph:cassDocGraph),
          new CassDocVertex(docId:rel.c1,cassDocGraph:cassDocGraph)
        ]
        return both.iterator()
    }
  }

}

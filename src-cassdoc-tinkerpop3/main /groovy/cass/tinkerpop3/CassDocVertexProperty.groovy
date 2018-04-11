package cass.tinkerpop3

import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex

import cassdoc.CassDocJsonUtil
import cassdoc.Detail
import cassdoc.OperationContext

class CassDocVertexProperty<V> extends CassDocProperty<V> {

  CassDocVertex vertex

  @Override
  boolean isPresent() {
    // this doesn't exist unless it is true...
    true
  }

  Object id() {
    return [vertex.docId, key] as String[]
  }

  <V> Property<V> property(String key, V value) {
    OperationContext opctx = new OperationContext(space:cassDocGraph.space)
    Detail detail = new Detail()
    StringWriter w = new StringWriter()
    CassDocJsonUtil.specialSerialize(value,w)
    cassDocGraph.cassDocAPI.newAttr(opctx, detail, docId, key, w.toString())
    CassDocProperty prop = new CassDocProperty(docId:docId,cassDocGraph:cassDocGraph,key:key,value:value)
    return prop
  }

  @Override
  Vertex element() {
    vertex
  }

  <U> Iterator<Property<U>> properties(String... propertyKeys) {
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
    Map<String,Object> attrMeta = cassDocGraph.cassDocAPI.deserializeAttrMetadata(opctx, detail, vertex.docId, key)
    String metadataId = attrMeta["_id"]

    for (Map.Entry<String,Object> entry : attrMeta.entrySet()) {
      CassDocProperty prop = new CassDocProperty(docId: metadataId,vertex:this)
      props.add(prop)
    }

    return props.iterator()
  }
}

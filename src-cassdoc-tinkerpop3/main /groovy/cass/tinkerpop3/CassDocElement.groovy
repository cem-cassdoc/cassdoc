package cass.tinkerpop3

import groovy.transform.CompileStatic

import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex

import cwdrg.lg.annotation.Log

@CompileStatic
@Log
abstract class CassDocElement implements Element {
  transient CassDocGraph cassDocGraph

  @Override
  Graph graph() {
    log.dbg("CassDoc: get graph",null,null)
    cassDocGraph
  }

  @Override
  abstract Object id()

  @Override
  abstract String label()


  @Override
  <V> Property<V> property(String key, V value) {
    log.dbg("CassDoc: add property invoked "+id(), null, null)
    throw Element.Exceptions.propertyAdditionNotSupported();
  }

  @Override
  abstract <V> Iterator<? extends Property<V>> properties(String... propertyKeys)


  @Override
  void remove() {
    log.dbg("CassDoc: remove element invoked "+id(), null, null)
    if (this instanceof Vertex)
      throw Vertex.Exceptions.vertexRemovalNotSupported();
    else
      throw Edge.Exceptions.edgeRemovalNotSupported();
  }
}

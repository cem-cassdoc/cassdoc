package cass.tinkerpop3

import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Property

class CassDocProperty<V> implements Property<V> {
  String key
  String typeCode
  V value

  String docId
  Element element

  transient CassDocGraph cassDocGraph

  @Override
  String key() {
    return key
  }

  @Override
  V value() throws NoSuchElementException {
    return value;
  }

  @Override
  boolean isPresent() {
    true;
  }

  @Override
  Element element() {
    element
  }

  @Override
  void remove() {
    throw Property.Exceptions.propertyRemovalNotSupported()
  }
}

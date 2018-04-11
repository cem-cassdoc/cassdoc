package cass.tinkerpop3

import java.util.concurrent.ConcurrentHashMap

import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper
import org.apache.tinkerpop.gremlin.structure.util.StringFactory


class CassDocGraphVariables implements Graph.Variables {

  private final Map<String, Object> variables = new ConcurrentHashMap<>();

  @Override
  Set<String> keys() {
    return this.variables.keySet();
  }

  @Override
  <R> Optional<R> get(final String key) {
    return Optional.ofNullable((R) this.variables.get(key));
  }

  @Override
  void remove(final String key) {
    this.variables.remove(key);
  }

  @Override
  void set(final String key, final Object value) {
    GraphVariableHelper.validateVariable(key, value);
    this.variables.put(key, value);
  }

  String toString() {
    return StringFactory.graphVariablesString(this);
  }
}

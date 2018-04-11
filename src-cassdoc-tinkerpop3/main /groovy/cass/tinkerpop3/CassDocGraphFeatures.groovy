package cass.tinkerpop3

import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures
import org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures
import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures


class CassDocGraphFeatures implements GraphFeatures {

  @Override
  boolean supportsConcurrentAccess() {
    return false;
  }

  @Override
  boolean supportsComputer() {
    return false;
  }

  @Override
  VariableFeatures variables() {
    return new CassDocVariableFeatures();
  }

  @Override
  boolean supportsPersistence() {
    return false;
  }

  @Override
  boolean supportsThreadedTransactions() {
    return false;
  }

  @Override
  boolean supportsTransactions() {
    return false;
  }
}


class CassDocElementFeatures implements ElementFeatures {

  @Override
  boolean supportsUserSuppliedIds() {
    return false;
  }

  @Override
  boolean supportsStringIds() {
    return false;
  }

  @Override
  boolean supportsUuidIds() {
    return false;
  }

  @Override
  boolean supportsAnyIds() {
    return false;
  }

  @Override
  boolean supportsCustomIds() {
    return false;
  }
}

class CassDocVertexFeatures implements VertexFeatures {
  @Override
  VertexPropertyFeatures properties() {
    return new CassDocVertexPropertyFeatures();
  }

  @Override
  boolean supportsMetaProperties() {
    return true
  }

  @Override
  boolean supportsMultiProperties() {
    return false
  }

  @Override
  boolean supportsUserSuppliedIds() {
    return false;
  }

  @Override
  boolean supportsAddVertices() {
    true
  }

  @Override
  boolean supportsRemoveVertices() {
    false
  }

  @Override
  VertexProperty.Cardinality getCardinality(final String key) {
    return VertexProperty.Cardinality.single
  }
}

class CassDocEdgeFeatures implements EdgeFeatures {
  @Override
  EdgePropertyFeatures properties() {
    return new CassDocEdgePropertyFeatures();
  }

  @Override
  boolean supportsAddEdges() {
    true
  }
  @Override
  boolean supportsRemoveEdges() {
    false
  }
}

class CassDocEdgePropertyFeatures implements EdgePropertyFeatures {


  @Override
  boolean supportsBooleanArrayValues() {
    return true
  }

  @Override
  boolean supportsBooleanValues() {
    return true
  }

  @Override
  boolean supportsByteArrayValues() {
    return true
  }

  @Override
  boolean supportsByteValues() {
    return true
  }

  @Override
  boolean supportsDoubleArrayValues() {
    return true
  }

  @Override
  boolean supportsDoubleValues() {
    return true
  }

  @Override
  boolean supportsFloatArrayValues() {
    return true
  }

  @Override
  boolean supportsFloatValues() {
    return true
  }

  @Override
  boolean supportsIntegerArrayValues() {
    return true
  }

  @Override
  boolean supportsIntegerValues() {
    return true
  }

  @Override
  boolean supportsLongArrayValues() {
    return true
  }

  @Override
  boolean supportsLongValues() {
    return true
  }

  @Override
  boolean supportsStringArrayValues() {
    return true
  }

  @Override
  boolean supportsStringValues() {
    return true
  }

  @Override
  boolean supportsMapValues() {
    return true
  }

  @Override
  boolean supportsMixedListValues() {
    return true
  }

  @Override
  boolean supportsSerializableValues() {
    return false;
  }

  @Override
  boolean supportsUniformListValues() {
    return true
  }

  @Override
  boolean supportsProperties() {
    return true
  }
}

class CassDocVariableFeatures implements Graph.Features.VariableFeatures {
  @Override
  boolean supportsVariables() {
    return false;
  }

  @Override
  boolean supportsBooleanValues() {
    return false;
  }

  @Override
  boolean supportsDoubleValues() {
    return false;
  }

  @Override
  boolean supportsFloatValues() {
    return false;
  }

  @Override
  boolean supportsIntegerValues() {
    return false;
  }

  @Override
  boolean supportsLongValues() {
    return false;
  }

  @Override
  boolean supportsMapValues() {
    return false;
  }

  @Override
  boolean supportsMixedListValues() {
    return false;
  }

  @Override
  boolean supportsByteValues() {
    return false;
  }

  @Override
  boolean supportsBooleanArrayValues() {
    return false;
  }

  @Override
  boolean supportsByteArrayValues() {
    return false;
  }

  @Override
  boolean supportsDoubleArrayValues() {
    return false;
  }

  @Override
  boolean supportsFloatArrayValues() {
    return false;
  }

  @Override
  boolean supportsIntegerArrayValues() {
    return false;
  }

  @Override
  boolean supportsLongArrayValues() {
    return false;
  }

  @Override
  boolean supportsStringArrayValues() {
    return false;
  }

  @Override
  boolean supportsSerializableValues() {
    return false;
  }

  @Override
  boolean supportsStringValues() {
    return true;
  }

  @Override
  boolean supportsUniformListValues() {
    return false;
  }
}



class CassDocVertexPropertyFeatures implements VertexPropertyFeatures {

  @Override
  boolean supportsMapValues() {
    return false;
  }

  @Override
  boolean supportsMixedListValues() {
    return true;
  }

  @Override
  boolean supportsSerializableValues() {
    return false;
  }

  @Override
  boolean supportsUniformListValues() {
    return true;
  }

  @Override
  boolean supportsUserSuppliedIds() {
    return false;
  }

  @Override
  boolean supportsAnyIds() {
    return false;
  }

  @Override
  boolean supportsBooleanArrayValues() {
    return true
  }

  @Override
  boolean supportsBooleanValues() {
    return true
  }

  @Override
  boolean supportsByteArrayValues() {
    return true
  }

  @Override
  boolean supportsByteValues() {
    return true
  }

  @Override
  boolean supportsDoubleArrayValues() {
    return true
  }

  @Override
  boolean supportsDoubleValues() {
    return true
  }

  @Override
  boolean supportsFloatArrayValues() {
    return true
  }

  @Override
  boolean supportsFloatValues() {
    return true
  }

  @Override
  boolean supportsIntegerArrayValues() {
    return true
  }

  @Override
  boolean supportsIntegerValues() {
    return true
  }

  @Override
  boolean supportsLongArrayValues() {
    return true
  }

  @Override
  boolean supportsLongValues() {
    return true
  }

  @Override
  boolean supportsStringArrayValues() {
    return true
  }

  @Override
  boolean supportsStringValues() {
    return true
  }

  //  @Override
  //  boolean supportsAdd() {
  //    return false;
  //  }
  //
  //  @Override
  //  boolean supportsRemove() {
  //    return false;
  //  }
}

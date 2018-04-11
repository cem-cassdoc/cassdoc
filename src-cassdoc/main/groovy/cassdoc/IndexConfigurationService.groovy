package cassdoc

import groovy.transform.CompileStatic
import cassdoc.commands.retrieve.IndexTableRP
import cassdoc.operations.SearchOperations


class IndexConfigurationService {
    Map<String, Index> indexes = [:]

    Index getIndex(String indexIdentifier) {
        indexes[indexIdentifier]
    }
}

interface Index {
    Iterator<Map> searchIndex(CommandExecServices svcs, OperationContext opctx, Detail detail, List lookupCriteria)
}

interface SearchFilter {
    boolean isFiltered(CommandExecServices svcs, OperationContext opctx, Detail detail, Map currentDocument)
}

@CompileStatic
class SimpleHasValueManualIndex {
    String name
    String i1, i2, i3

    Iterator<Map> searchIndex(CommandExecServices svcs, OperationContext opctx, Detail detail, List lookupCriteria) {
        IndexTableRP rp = new IndexTableRP(i1: i1, i2: i2, i3: i3, k1: lookupCriteria[0].toString())
        rp.initiateQuery(svcs, opctx, detail)
        Iterator<Map> iterator = SearchOperations.pullIDResultSet(svcs, opctx, detail, rp)
        return iterator
    }
}

// this should typically be on the e_ table and indexing a FixedAttr
@CompileStatic
class SecondaryIndex {
    String name
    String dbname
    String table
    String column
    String columnType

    Iterator<Map> searchIndex(CommandExecServices svcs, OperationContext opctx, Detail detail, List lookupCriteria) {
    }
}

@CompileStatic
class MaterializedViewIndex {
    String name
    String dbname
    String table
    String column
    String columnType

    Iterator<Map> searchIndex(CommandExecServices svcs, OperationContext opctx, Detail detail, List lookupCriteria) {
    }
}

// most recent index, elissandra,

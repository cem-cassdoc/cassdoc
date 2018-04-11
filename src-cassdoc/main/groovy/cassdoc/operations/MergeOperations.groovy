package cassdoc.operations

import groovy.transform.CompileStatic

@CompileStatic
class MergeOperations {

    void updatePreserveChildren(String newJSON) {
        // get child relations
        // keep proper ids if curently in child relation set
        // add new ones as encountered
        // delete child relations that are no longer in attribute's content
    }

    void unionAttrs() {
    }

    void appendToArray(String arrayLocationPath, Reader inputJsonArray) {
        // inputJsonArray may be subdocuments, so we'll need to parse and stream it.

    }
}

// TODO: These do complicated merge operations on single properties, subdocs, docs, etc. Think set operations with nuances...

/* recursive update structure?
 array operations: replace all, empty, append, update entries(), expand, apply groovy closure, find, findAll, etc
 map operations: find, findAll, remove, removeAll, addAll
 */
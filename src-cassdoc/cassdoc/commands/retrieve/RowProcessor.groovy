package cassdoc.commands.retrieve

import groovy.transform.CompileStatic
import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.OperationContext

@CompileStatic
abstract class RowProcessor {
  boolean newPartition
  long pageCount = 0
  long rowCount = 0
  long partitionRowCount = 0
  int fetchNextPageThreshold = 5000
  void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args) {}

  /**
   * returns the result Object[] array from processing the next row from the backend datastore. 
   * 
   * The returned Object[] array should have absolutely no datastore-specific objects or object wrappers. 
   * 
   * @return
   */
  Object[] nextRow() {}

  /**
   * This optional method initializes any data structures tracking data across column/clustering keys in a partition
   *
   */
  void initNewPartition() {}

  /**
   * This optional method is called whenever a new partition key is encountered,  in case
   * there are some final products/packaging/processing needed before the next partition tracking is done
   *
   * Examples: summing columns in a row, tracking column keys in a row, etc
   */
  void completeFinishedPartition() {}

  /**
   * An optional method, the code using the RowProcessor, if indicated by the newPartition stateful property, can call this method to get the
   * final products/data structures/information that has been accumulated and finalized by completeFinishedPartition()
   *
   * @return Object[]
   */
  Object[] getFinishedPartitionData() {
    null
  }


  List<Object[]> getAllRows() {
    List rows = []
    Object[] row = null
    while (row = nextRow()) {
      rows.add(row)
    }
    return rows
  }
}

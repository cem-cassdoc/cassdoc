package cassdoc.commands.retrieve.cassandra

import groovy.transform.CompileStatic

import org.apache.commons.lang3.StringUtils

import cassdoc.Detail
import cassdoc.OperationContext
import cassdoc.commands.retrieve.RowProcessor

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Token


/**
 * base class for most cassandra row processors, handles the main issues of partition detection, paging / streaming, etc.
 * 
 * @author cowardlydragon
 *
 */

@CompileStatic
abstract class CassandraPagedRowProcessor extends RowProcessor {
  static String resolveConsistency(Detail detail, OperationContext opctx) {
    if (detail != null && StringUtils.isNotEmpty(detail.readConsistency))
      return detail.readConsistency
    if (StringUtils.isNotEmpty(opctx.readConsistency))
      return opctx.readConsistency
    return "ONE"
  }


  ResultSet rs = null
  private Token lastToken = null

  /**
   * This method is called for every row encountered. This performs the cassandra-specific row processing to convert
   * the row to the Object[] for the row. Do not place cassandra-specific objects/classes into the Object[] row. 
   * 
   * @param row
   * @return
   */
  Object[] processRow(Row row){}


  Object[] nextRow() {
    Row row = rs.one()
    if (row == null) {
      newPartition = false
      return null
    } else {
      rowCount++
      partitionRowCount++
      Token currentToken = row.partitionKeyToken
      if (currentToken == lastToken) {
        newPartition = false
        if (lastToken == null) {
          pageCount++ // increment to 1 as soon as we get a legitimate row
          lastToken = currentToken
        } else {
          newPartition = true
          completeFinishedPartition()
          lastToken = currentToken
          initNewPartition()
        }
      }

      if (rs.getAvailableWithoutFetching() == fetchNextPageThreshold && !rs.isFullyFetched()) {
        pageCount++;
        rs.fetchMoreResults()
      }
    }

    return processRow(row)
  }
}
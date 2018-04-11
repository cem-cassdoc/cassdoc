package cassdoc.operations

import groovy.transform.CompileStatic

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import cassdoc.CommandExecServices
import cassdoc.Detail
import cassdoc.IDUtil
import cassdoc.OperationContext
import cassdoc.commands.retrieve.RowProcessor
import cassdoc.commands.retrieve.cassandra.CassandraPagedRowProcessor

import com.datastax.driver.core.*

import cwdrg.lg.annotation.Log
import cwdrg.util.async.iterator.BlockingIterator
import drv.cassdriver.CQLException
import drv.cassdriver.RowCallbackHandler
import drv.cassdriver.St


// TODO rewrite using CassandraPagedRowProcessors

@Log
@CompileStatic
class SearchOperations {

  /** by selecting the token values for 10 rowkeys, we can estimate the table rows/density
   * 
   * 
   * @param startToken
   * @param stopToken
   * @param rangeCount
   * @return
   */

  static void searchTableIdListJSONArray(CommandExecServices svcs, OperationContext opctx, Detail detail, String objectType, Writer w) {
    w << "["
    scanETable(svcs,opctx,detail,objectType,null,null,null,new IDListJSONArrayFromETableRCH(w:w))
    w << "]"
  }

  // source: set or list iterator of ids...
  // async iterator: http://stackoverflow.com/questions/21143996/asynchronous-iterator
  // IN clauses are not token-aware from the java-driver :-( , the id set is sent to a coordinator node, parallel is better
  static void searchIdsIterator(CommandExecServices svcs, OperationContext opctx, Detail detail, Iterator<String> ids, Writer w)
  {
    w << '['
    boolean first = true
    for (String id : ids) {
      // TODO: type filters, etc
      if (first) first = false; else w << ",";
      RetrievalOperations.getSingleDoc(svcs, opctx, detail, id, w, true)
      // TODO: parallelism
    }
    w << ']'
  }


  class ScanEntityTableBase extends CassandraPagedRowProcessor
  {
    String startToken
    String stopToken
    List<String> fixedCols
    String objectType
    Object[] cqlargs
    void initiateQuery(CommandExecServices svcs, OperationContext opctx, Detail detail, Object... args)
    {
      String consistency = resolveConsistency(detail,opctx)
      String space = opctx.space
      String suffix = svcs.typeSvc.getSuffixForType(objectType)
      startToken = startToken ?: detail.searchStartToken
      stopToken = stopToken ?: detail.searchStopToken

      StringBuilder tableQuery = new StringBuilder("SELECT token(e),e")
      if (fixedCols != null) {
        for (String col : fixedCols) { tableQuery << "," << col  }
      }
      tableQuery <<  " FROM ${space}.e_${suffix}"
      boolean hasWhere = false
      Object[] tokenRangePrepArgs = null
      if (startToken != null || stopToken != null) {
        hasWhere = true
        tableQuery << " WHERE "
        if (startToken != null) {
          tableQuery << "token(e) >= ? "
          if (stopToken != null) {
            tableQuery << " AND "
            tokenRangePrepArgs = [
              new Long(startToken),
              new Long(stopToken)] as Object[]
          } else {
            tokenRangePrepArgs = [new Long(startToken)] as Object[]
          }
        }
        if (stopToken != null) {
          tableQuery << "token(e) <= ? "
          if (tokenRangePrepArgs == null) {
            tokenRangePrepArgs = [new Long(stopToken)] as Object[]
          }
        }
      }
      rs = svcs.driver.initiateQuery(space, tableQuery.toString(), cqlargs, consistency, detail?.fetchPageSize ?: 30000, detail?.fetchNextPageThreshold ?: 3000)
    }
  }

  // one row == one entity/doc id, should be pretty simple
  static void scanETable(CommandExecServices svcs, OperationContext opctx, Detail detail, String objectType, List<String> fixedCols, String startToken, String stopToken, RowCallbackHandler rch)
  {
    String space = opctx.space
    String suffix = svcs.typeSvc.getSuffixForType(objectType)
    startToken = startToken ?: detail.searchStartToken
    stopToken = stopToken ?: detail.searchStopToken

    StringBuilder tableQuery = new StringBuilder("SELECT token(e),e")
    if (fixedCols != null) {
      for (String col : fixedCols) { tableQuery << "," << col  }
    }
    tableQuery <<  "FROM ${space}.e_${suffix}"
    boolean hasWhere = false
    Object[] tokenRangePrepArgs = null
    if (startToken != null || stopToken != null) {
      hasWhere = true
      tableQuery << " WHERE "
      if (startToken != null) {
        tableQuery << "token(e) >= ? "
        if (stopToken != null) {
          tableQuery << " AND "
          tokenRangePrepArgs = [
            new Long(startToken),
            new Long(stopToken)] as Object[]
        } else {
          tokenRangePrepArgs = [new Long(startToken)] as Object[]
        }
      }
      if (stopToken != null) {
        tableQuery << "token(e) <= ? "
        if (tokenRangePrepArgs == null) {
          tokenRangePrepArgs = [new Long(stopToken)] as Object[]
        }
      }
    }

    // TODO: allow filtering, cluster key subsets, etc

    int fetchNextPageThreshold = detail?.fetchNextPageThreshold ?: 3000
    int fetchPageSize = detail?.fetchPageSize ?: 30000

    SimpleStatement stmt = new SimpleStatement(tableQuery.toString())
    stmt.setFetchSize(fetchPageSize)
    stmt.setConsistencyLevel(svcs.driver.getConsistencyLevel(detail.readConsistency))
    St st = new St(stmt:stmt,cql:tableQuery.toString(),cqlargs:tokenRangePrepArgs,keyspace:space)
    ResultSet cassRS = svcs.driver.executeStatementSync(svcs.driver.getSession(), st)

    long rowCount = 0
    long pageCount = 1
    for (Row curDBRow : cassRS)
    {
      rch.processRow(curDBRow)
      rowCount++
      if (cassRS.getAvailableWithoutFetching() == fetchNextPageThreshold && !cassRS.isFullyFetched()) {
        pageCount++;
        cassRS.fetchMoreResults()
      }
    }
  }

  // PTabelBaseRCH has both per-row and processDoc event methods
  static void scanPTable(CommandExecServices svcs, OperationContext opctx, Detail detail, String objectType, String startToken, String stopToken, PTableBaseRCH rch)
  {
    String space = opctx.space
    String suffix = svcs.typeSvc.getSuffixForType(objectType)
    startToken = startToken ?: detail.searchStartToken
    stopToken = stopToken ?: detail.searchStopToken

    StringBuilder tableQuery = new StringBuilder("SELECT token(e),e,p,t,d FROM ${space}.p_${suffix}")
    Object[] tokenRangePrepArgs = null
    if (startToken != null || stopToken != null) {
      tableQuery << " WHERE "
      if (startToken != null) {
        tableQuery << "token(e) >= ? "
        if (stopToken != null) {
          tableQuery << " AND "
          tokenRangePrepArgs = [
            new Long(startToken),
            new Long(stopToken)] as Object[]
        } else {
          tokenRangePrepArgs = [new Long(startToken)] as Object[]
        }
      }
      if (stopToken != null) {
        tableQuery << "token(e) <= ? "
        if (tokenRangePrepArgs == null) {
          tokenRangePrepArgs = [new Long(stopToken)] as Object[]
        }
      }
    }

    // TODO: allow filtering, cluster key subsets, etc

    int fetchNextPageThreshold = detail?.fetchNextPageThreshold ?: 3000
    int fetchPageSize = detail?.fetchPageSize ?: 30000


    SimpleStatement stmt = new SimpleStatement(tableQuery.toString())
    stmt.setFetchSize(fetchPageSize)
    stmt.setConsistencyLevel(svcs.driver.getConsistencyLevel(detail.readConsistency))
    St st = new St(stmt:stmt,cql:tableQuery.toString(),cqlargs:tokenRangePrepArgs,keyspace:space)
    ResultSet cassRS = svcs.driver.executeStatementSync(svcs.driver.getSession(), st)

    long rowCount = 0
    long pageCount = 1
    for (Row curDBRow : cassRS)
    {
      rch.processRow(curDBRow)
      rowCount++
      if (cassRS.getAvailableWithoutFetching() == fetchNextPageThreshold && !cassRS.isFullyFetched()) {
        pageCount++;
        cassRS.fetchMoreResults()
      }
    }
    // process the very last doc
    rch.processDoc()
  }

  // rp.processRow() should return token in 0th and id in 1st cell
  public static Iterator<Map> pullIDResultSet(final CommandExecServices svcs, final OperationContext opctx, final Detail detail, final RowProcessor rp)
  {
    final LinkedBlockingQueue docQ = new LinkedBlockingQueue(1000)
    final BlockingIterator<Map> iterator = new BlockingIterator<Map>(queue:docQ)

    new Thread() {
          public void run() {
            Object rowdata = null
            while (rowdata = rp.nextRow()) {
              String id = (String)rowdata[1]
              Map doc = RetrievalOperations.deserializeSingleDoc(svcs, opctx, detail, id, true)
              docQ.put(doc)
            }

          }
        }
    return iterator
  }

  static void retrieveIDListThreadPoolExecutor(CommandExecServices svcs, OperationContext opctx, Detail detail, Iterator<String> idIterator, Writer writer)
  {
    LinkedBlockingQueue idQueue = new LinkedBlockingQueue()
    String uuid = IDUtil.timeUUID()
    int i = 0;
    writer << '['

    ExecutorService execSvc = Executors.newFixedThreadPool(5)

    // luckily this is only used in the already-synchronized mainWriter
    Boolean[] first = [true] as Boolean[]

    // only issue with this is we may flood the underlying executorservice queue with ids... is there a throttle we can do?
    // - throttle may need to be further upsteam... perhaps in the Object stream code, or what is feeding the queue.
    while (idIterator.hasNext())
    {
      String docUUID = idIterator.next()
      DocWriterTask task = new DocWriterTask(svcs:svcs,space:opctx.space,detail:detail,mainWriter:writer,docUUID:docUUID,first:first)
      task.setName(uuid+i)
      i++
      execSvc.execute(task)
    }

    execSvc.shutdown()
    execSvc.awaitTermination(10, TimeUnit.MINUTES)
    writer << ']'
    log.dbg("DONE", null)
  }

  static void retrieveIDList(CommandExecServices svcs, OperationContext opctx, Detail detail, Iterator<String> idIterator, Writer writer)
  {
    LinkedBlockingQueue idQueue = new LinkedBlockingQueue()
    String uuid = IDUtil.timeUUID()
    int i = 0;
    writer << '['

    ExecutorService execSvc = Executors.newFixedThreadPool(5)

    // luckily this is only used in the already-synchronized mainWriter
    Boolean[] first = [true] as Boolean[]

    // only issue with this is we may flood the underlying executorservice queue with ids... is there a throttle we can do?
    // - throttle may need to be further upsteam... perhaps in the Object stream code, or what is feeding the queue.
    while (idIterator.hasNext())
    {
      String docUUID = idIterator.next()
      DocWriterTask task = new DocWriterTask(svcs:svcs,space:opctx.space,detail:detail,mainWriter:writer,docUUID:docUUID,first:first)
      task.setName(uuid+i)
      i++
      execSvc.execute(task)
    }

    execSvc.shutdown()
    execSvc.awaitTermination(10, TimeUnit.MINUTES)
    writer << ']'
    log.dbg("DONE", null)
  }


}



@CompileStatic
@Log
class DocWriterTask extends Thread
{
  CommandExecServices svcs
  String space
  Detail detail
  Writer mainWriter
  String docUUID
  Boolean[] first = null


  void run() {
    log.dbg("id: $docUUID", null)
    StringWriter docWriter = new StringWriter()
    OperationContext opctx = new OperationContext(space:space)
    RetrievalOperations.getSingleDoc(svcs, opctx, detail, docUUID, docWriter, true)
    log.dbg("writemain START $docUUID", null)
    writeMain(docWriter,mainWriter,first)
    log.dbg("writemain STOP $docUUID", null)
  }

  static void writeMain(StringWriter docWriter, Writer mainWriter, Boolean[] first)
  {
    synchronized (mainWriter) {
      log.dbg("WRITE TO MAINWRITER: "+docWriter.toString(),null)
      if (first[0]) first[0] = false else mainWriter << ","
      mainWriter.write(docWriter.toString())
    }
  }
}



@CompileStatic
abstract class PTableBaseRCH
{
  // called when the current doc is done being scanned
  public abstract void processDoc()

  // called on a per-attr basis
  public abstract void processAttrRow(Row row)

  String currentDocUUID = null
  public void processRow(Row row) throws CQLException
  {
    String uuid = row.getString(1)

    if (currentDocUUID != uuid) {
      // I do it this way to try to avoid doing the null tests for every row
      if (currentDocUUID != null) { processDoc() }
      currentDocUUID = uuid
    }
    processAttrRow(row)
  }
}

@CompileStatic
class IDListJSONArrayFromETableRCH implements RowCallbackHandler
{
  Writer w
  boolean first = true
  public void processRow(Row row) throws CQLException
  {
    if (first) first = false; else w << ","
    String id = row.getString(1)
    w << '"' << id << '"'
  }

}








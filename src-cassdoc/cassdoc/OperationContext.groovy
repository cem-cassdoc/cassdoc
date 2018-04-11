package cassdoc

import groovy.transform.CompileStatic
import cassdoc.commands.mutate.cassandra.MutationCmd
import cassdoc.exceptions.PersistenceConflictException
import cassdoc.exceptions.UnexpectedPersistenceStateException

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row

import cwdrg.lg.annotation.Log
import cwdrg.util.json.JSONUtil
import drv.cassdriver.St


@Log
@CompileStatic
class OperationContext {

  String space

  void batchInit() {
    batches = [:]
    batchLeftovers = []
  }

  int traceLevel = -1 // 1 is trace, 2 is debug, 3 is info, 4 is warn, 5 is error
  Queue<String> trace = null

  boolean cqlTraceEnabled = false
  void setCqlTraceEnabled(boolean val) { if (val) cqlTrace = []; cqlTraceEnabled = val }
  List<Object[]> cqlTrace = null

  String readConsistency = "LOCAL_QUORUM"
  String writeConsistency = "LOCAL_QUORUM"

  String name
  Object security

  String[] paxosGatekeeperUpdateID = null // P|R|E + docUUID

  List<MutationCmd> commands = []
  Set deletedIds = [] as Set
  Set deletedIdAttrs = [] as Set

  Map<String,List<MutationCmd>> batches
  List<MutationCmd> batchLeftovers

  Long operationTimestamp = new Date().time * 1000 // Microseconds ??
  Long operationClearTimestamp = operationTimestamp - 1000
  // -- remember the timestamp -1 clear trick
  // -- java 9 will get nanoprecision time
  // -- cheap lock: timestamp + 30 seconds, hmmmmm
  UUID updateUUID = IDUtil.timeuuid()
  //UUID clearUUID // ?different UUID for clears?


  String executionMode = "immediate"  // immediate async, immediate sync, batch/spray async/sync
  boolean updateAsyncMode  // sync vs async version of executionMode... ?may be influenced by detail?

  void setExecutionMode(String mode) {
    if (mode == "batch") {
      batchInit()
    }
    executionMode = mode
  }

  void addCommand(CommandExecServices svcs, Detail detail, MutationCmd cmd) {
    if (cmd.added) {
      log.dbg("ALREADY ADDED: ${cmd.class.name} "+JSONUtil.serialize(cmd),null)
      return
    }
    cmd.added = true
    if (executionMode == "immediate") {
      log.dbg("EXEC IMMEDIATE: "+JSONUtil.serialize(cmd),null)
      cmd.execMutation(svcs, this, detail)
    } else {
      if (cmd.optimize(svcs, this, detail)) {
        commands.add(cmd)
      } else {
        log.dbg("FILTERED CMD: "+JSONUtil.serialize(cmd),null)
      }
    }
  }

  void setExecutionModeToImmediate() { executionMode = "immediate" }
  void setExecutionModeToBatch() { executionMode = "batch" }


  void DO(CommandExecServices svcs, Detail detail) {
    // TODO: optimize: batches, elimination of unneeded smaller ops if there is an overarching DELETE
    // TODO: PAXOS ops are gateway-checked, then other attendant updates to rel and idx tables can be done, subents, etc
    if (executionMode != "immediate") {
      if (executionMode == "batch") {
        commands.each { it.batch(this) }
        if (paxosGatekeeperUpdateID != null) {
          String paxosBatchID = paxosGatekeeperUpdateID[0]+paxosGatekeeperUpdateID[1]
          List<MutationCmd> paxosBatchCmds = batches[paxosBatchID]
          batches.remove(paxosBatchID)
          List paxosBatch = new ArrayList<>(paxosBatchCmds.size())
          if (cqlTraceEnabled) cqlTrace.add([
            "BEGIN BATCH",
            null,
            detail.writeConsistency,
            operationTimestamp] as Object[])
          // prepare the stmts
          paxosBatchCmds.eachWithIndex { MutationCmd cmd, int i ->
            St bst = (St)paxosBatchCmds[i].execMutation(svcs, this, detail)
            if (cqlTraceEnabled) cqlTrace.add([
              bst.cql,
              bst.cqlargs,
              detail.writeConsistency,
              operationTimestamp] as Object[])
            paxosBatch[i] = bst
          }
          if (cqlTraceEnabled) cqlTrace.add([
            "END BATCH",
            null,
            detail.writeConsistency,
            operationTimestamp] as Object[])
          ResultSet paxosResultSet = svcs.driver.executeSyncStatementBatch(paxosBatch, detail.writeConsistency, operationTimestamp)
          if (paxosResultSet == null) {
            // that's a problem
            throw log.err("",new UnexpectedPersistenceStateException("Paxos BATCH update return result is null "+JSONUtil.serialize(paxosBatch)))
          } else {
            Row row = paxosResultSet.one();
            Boolean paxosResult = row.getBool(0)
            log.dbg("pax result: ${row.getColumnDefinitions().getName(0)} :: $paxosResult",null)
            if (!paxosResult) {
              throw log.err("", new PersistenceConflictException("Paxos BATCH update did not occure, indicated version conflict "+JSONUtil.serialize(paxosBatch)))
            }

          }
          // if that is successful (how would we know)? execute the other batches
        }
        batches.values().each { List<MutationCmd> batchCmds ->
          List stmtBatch = new ArrayList<>(batchCmds.size())
          // prepare the stmts
          if (cqlTraceEnabled) cqlTrace.add([
            "BEGIN BATCH",
            null,
            detail.writeConsistency,
            operationTimestamp] as Object[])
          batchCmds.eachWithIndex { MutationCmd cmd, int i ->
            St bst = (St)batchCmds[i].execMutation(svcs, this, detail)
            if (cqlTraceEnabled) cqlTrace.add([
              bst.cql,
              bst.cqlargs,
              detail.writeConsistency,
              operationTimestamp] as Object[])
            stmtBatch[i] = bst
          }
          if (cqlTraceEnabled) cqlTrace.add([
            "END BATCH",
            null,
            detail.writeConsistency,
            operationTimestamp] as Object[])
          svcs.driver.executeSyncStatementBatch(stmtBatch, detail.writeConsistency, operationTimestamp)
        }
        batchLeftovers.each { MutationCmd cmd ->
          if (cmd != null) {
            St stmt = (St)cmd.execMutation(svcs,this,detail);
            if (cqlTraceEnabled) cqlTrace.add([
              stmt.cql,
              stmt.cqlargs,
              detail.writeConsistency,
              cmd.clearCmd ? operationClearTimestamp : operationTimestamp] as Object[])
            svcs.driver.executeSyncStatement(stmt)
          }
        }
      } else {
        // we assume "spray" TODO: async
        if (paxosGatekeeperUpdateID != null) {
          MutationCmd paxosCmd = null
          for (int i=0; i < commands.size(); i++) {
            paxosCmd = commands[i]
            if (paxosCmd.paxosId != null && paxosCmd.paxosId == paxosGatekeeperUpdateID) {
              log.dbg("MODE: sync PAXOS id found",null)
              // exec this
              St stmt = (St)paxosCmd.execMutation(svcs,this,detail)
              if (cqlTraceEnabled) cqlTrace.add([
                stmt.cql,
                stmt.cqlargs,
                detail.writeConsistency,
                paxosCmd.clearCmd ? operationClearTimestamp : operationTimestamp] as Object[])
              ResultSet paxosResultSet = svcs.driver.executeSyncStatement(stmt)
              if (paxosResultSet == null) {
                // that's a problem
                throw log.err("",new UnexpectedPersistenceStateException("Paxos update return result is null "+JSONUtil.serialize(stmt)))
              } else {
                Row row = paxosResultSet.one();
                Boolean paxosResult = row.getBool(0)
                log.dbg("pax result: ${row.getColumnDefinitions().getName(0)} :: $paxosResult",null)
                if (!paxosResult) {
                  throw log.err("", new PersistenceConflictException("Paxos update did not occure, indicated version conflict "+JSONUtil.serialize(stmt)))
                }

              }

              // clear it
              commands.set(i,null)
              break;
            }
          }
        }
        commands.each { cmd ->
          if (cmd != null) {
            if (cmd.optimize(svcs, this, detail)) {
              St stmt = (St)cmd.execMutation(svcs,this,detail);
              if (cqlTraceEnabled) cqlTrace.add([
                stmt.cql,
                stmt.cqlargs,
                detail.writeConsistency,
                cmd.clearCmd ? operationClearTimestamp : operationTimestamp] as Object[])
              svcs.driver.executeSyncStatement(stmt)
            }
          }
        }
      }
    }
  }
}

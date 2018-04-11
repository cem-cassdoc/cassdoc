package drv.cassdriver

import groovy.transform.CompileStatic

import com.datastax.driver.core.Statement

@CompileStatic
class St {
  Statement stmt
  String keyspace
  String cql
  Object[] cqlargs
}
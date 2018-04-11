package drv.cassdriver;

import com.datastax.driver.core.Row;

public interface RowCallbackHandler {

  void processRow(Row row) throws CQLException;

}

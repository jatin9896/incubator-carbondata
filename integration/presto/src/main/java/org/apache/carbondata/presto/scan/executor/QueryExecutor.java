package org.apache.carbondata.presto.scan.executor;

import java.io.IOException;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;

/**
 * Interface for carbon query executor.
 * Will be used to execute the query based on the query model
 * and will return the iterator over query result
 */
public interface QueryExecutor<E> {

  /**
   * Below method will be used to execute the query based on query model passed from driver
   *
   * @param queryModel query details
   * @return query result iterator
   * @throws QueryExecutionException if any failure while executing the query
   * @throws IOException if fail to read files
   */
  CarbonIterator<E> execute(QueryModel queryModel)
      throws QueryExecutionException, IOException;

  /**
   * Below method will be used for cleanup
   *
   * @throws QueryExecutionException
   */
  void finish() throws QueryExecutionException;
}


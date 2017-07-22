package org.apache.carbondata.presto.impl

import java.io.IOException

import scala.util.Try

import org.apache.carbondata.core.cache.Cache
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.cache.CacheType
import org.apache.carbondata.core.cache.dictionary.Dictionary
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport

/**
 * This is the class to decode dictionary encoded column data back to its original value.
 */
class PrestoDictionaryDecodeReadSupport[T] extends CarbonReadSupport[T] {
  protected var dictionaries: Array[Dictionary] = null
  protected var dataTypes: Array[DataType] = null
  /**
   * carbon columns
   */
  protected var carbonColumns: Array[CarbonColumn] = null

  /**
   * This initialization is done inside executor task
   * for column dictionary involved in decoding.
   *
   * @param carbonColumns           column list
   * @param absoluteTableIdentifier table identifier
   */
  @throws[IOException]
  def initialize(carbonColumns: Array[CarbonColumn],
      absoluteTableIdentifier: AbsoluteTableIdentifier) {
    this.carbonColumns = carbonColumns
    dictionaries = new Array[Dictionary](carbonColumns.length)
    dataTypes = new Array[DataType](carbonColumns.length)
    var i: Int = 0
    while (i < carbonColumns.length) {
      {
        if (carbonColumns(i).hasEncoding(Encoding.DICTIONARY) &&
            !carbonColumns(i).hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !carbonColumns(i).isComplex) {
          val cacheProvider: CacheProvider = CacheProvider.getInstance
          val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
            cacheProvider
              .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath)
          dataTypes(i) = carbonColumns(i).getDataType
          dictionaries(i) = forwardDictionaryCache
            .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier
              .getCarbonTableIdentifier, carbonColumns(i).getColumnIdentifier, dataTypes(i)))
        }
        else {
          dataTypes(i) = carbonColumns(i).getDataType
        }
      }
      { i += 1; i - 1 }
    }
  }

  def readRow(data: Array[AnyRef]): T = {
    var i: Int = 0
    while (i < dictionaries.length) {
      {
        if (Try(data(i).toString.toInt).toOption.isDefined) {
          if (dictionaries(i) != null) {
            data(i) = dictionaries(i).getDictionaryValueForKey(data(i).asInstanceOf[Int])
          }
        }
        { i += 1; i - 1 }
      }
    }
    data.asInstanceOf[T]
  }

  /**
   * to book keep the dictionary cache or update access count for each
   * column involved during decode, to facilitate LRU cache policy if memory
   * threshold is reached
   */
  def close() {
    if (dictionaries == null) {
      return
    }
    var i: Int = 0
    while (i < dictionaries.length) {
      {
        CarbonUtil.clearDictionaryCache(dictionaries(i))
      }
      { i += 1; i - 1 }
    }
  }
}

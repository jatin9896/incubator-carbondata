package org.apache.carbondata.presto.impl

import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
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
  protected var dictionaries: Array[Dictionary] = _
  protected var dataTypes: Array[DataType] = _

  /**
   * This initialization is done inside executor task
   * for column dictionary involved in decoding.
   *
   * @param carbonColumns           column list
   * @param absoluteTableIdentifier table identifier
   */

  override def initialize(carbonColumns: Array[CarbonColumn],
      absoluteTableIdentifier: AbsoluteTableIdentifier) {

    dictionaries = new Array[Dictionary](carbonColumns.length)
    dataTypes = new Array[DataType](carbonColumns.length)

    carbonColumns.zipWithIndex.foreach {
      case (carbonColumn, index) => if (carbonColumn.hasEncoding(Encoding.DICTIONARY) &&
                                        !carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
                                        !carbonColumn.isComplex) {
        val cacheProvider: CacheProvider = CacheProvider.getInstance
        val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
          cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath)
        dataTypes(index) = carbonColumn.getDataType
        dictionaries(index) = forwardDictionaryCache
          .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier
            .getCarbonTableIdentifier, carbonColumn.getColumnIdentifier, dataTypes(index)))
      }
      else {
        dataTypes(index) = carbonColumn.getDataType
      }
    }

  }


  override def readRow(data: Array[AnyRef]): T = {
    throw  new RuntimeException("UnSupported Method Call Convert Column Instead")
  }

  def convertColumn(data: Array[AnyRef], columnNo: Int): T = {
    val convertedData: Any = if (Option(dictionaries(columnNo)).isDefined) {
      data.map { value => dictionaries(columnNo).getDictionaryValueForKey(value.asInstanceOf[Int]) }
    } else {
      data
    }
    convertedData.asInstanceOf[T]
  }

  /**
   * to book keep the dictionary cache or update access count for each
   * column involved during decode, to facilitate LRU cache policy if memory
   * threshold is reached
   */
  def close() {
    dictionaries.foreach(dictionary => if (Option(dictionary).isDefined) {
      CarbonUtil.clearDictionaryCache(dictionary)
    })
  }
}

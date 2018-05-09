/*
 * MIT License
 *
 * Copyright (c) 2018 Crown Copyright
 *                    Animal and Plant Health Agency
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package sparkutil

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

trait DataFrameImplicits {
  implicit class DataFrameEnrichment(df: DataFrame){
    // From https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex
    def zipWithIndex(session: SparkSession): DataFrame = {
      val newRows =  df.rdd.zipWithIndex.map{case (line, idx) =>
        Row.fromSeq(idx +: line.toSeq)
      }
      val newStruct = StructType(StructField("id", LongType, nullable = false) +: df.schema.fields)

      session.createDataFrame(
        newRows,
        newStruct
      )
    }

    def asTimestampColumn(colName: String, timeFormat: String, nullable: Boolean = false): DataFrame = {
      df.withColumn(colName, functions.unix_timestamp(df.col(colName), timeFormat))
          .setColumnNullability(nullable, colName)
    }

    //Based on https://stackoverflow.com/questions/33193958/change-nullable-property-of-column-in-spark-dataframe
    def setAllColumnNullability(nullable: Boolean): DataFrame = setColumnNullability(nullable, df.schema.fieldNames: _*)
    def setColumnNullability(nullable: Boolean, columnNames: String *): DataFrame = {
      val schema = df.schema
      val newSchema = StructType(schema.map {
        case StructField( c, t, _, m) if columnNames.contains(c) => StructField( c, t, nullable, m)
        case s: StructField => s
      })
      df.sqlContext.createDataFrame( df.rdd, newSchema )
    }
  }
}

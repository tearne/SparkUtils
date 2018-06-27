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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait DatasetImplicits {
  implicit class DSEnrichment[P](ds1: Dataset[P]) extends Serializable {
    /**
      * Support for type safe joining
      * @param ds2 Dataset to join with
      * @param joinType Join type (default == 'inner').  See https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-joins.html
      * @param f Extractor for left join key
      * @param g Extractor for right join key
      * @return Typed Tuple2 Dataset
      */
    def joinSafe[Q, R] // Based on https://stackoverflow.com/questions/40605167/perform-a-typed-join-in-scala-with-spark-datasets
        (ds2: Dataset[Q], joinType: String = "inner")
        (f: P => R, g: Q => R)
        (implicit e1: Encoder[(R, P)], e2: Encoder[(R, Q)], e3: Encoder[(P, Q)]): Dataset[(P, Q)] = {

      val t1 = ds1.map(v => f(v) -> v)
      val t2 = ds2.map(v => g(v) -> v)
      def get[S](value: (_,S)): S = if(value == null) null.asInstanceOf[S] else value._2
      t1
          .joinWith(t2, t1.col("_1") === t2.col("_1"), joinType)
          .map{case (left, right) => get(left) -> get(right)}
    }

    def sortedBy[Q]
        (f: P => Q)
        (implicit e1: Encoder[P], e2: Encoder[(Q, P)]): Dataset[P] = {

      ds1.map(d => (f(d), d)).orderBy("_1").map(_._2)
    }

    def zipWithIndex(session: SparkSession)(implicit e1: Encoder[(P, Long)]): Dataset[(P, Long)] = {
      session.createDataset(ds1.rdd.zipWithIndex())
    }
  }


}

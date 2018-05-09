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

package sparkutil.example

import org.apache.spark.sql.SparkSession
import sampler._

object Sorting extends App {
  val session: SparkSession = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

  import session.implicits._

  case class MyData(i: Int, value: Double)

  val length = 10
  val unsorted = (1 to length)
      .map(i => MyData(i, math.random.decimalPlaces(1)))
      .toDS

  unsorted.show(length)

  val sorted = unsorted.sortedBy(_.value)

  sorted.show(length)
}

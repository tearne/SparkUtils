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

import java.nio.file.Paths

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Random

object Joining extends App {
  val session: SparkSession = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

  import session.implicits._

  val bigNumber = 99999

  val outFile = Paths.get("out", "mergeTest.csv")

  case class Observed(id: Int, count: Int)
  case class Details(id: Int, tagNumber: Int)
  case class JoinedDetails(id: Int, count: Int, tagNumber: Int)

  val randomObservedDataset: Dataset[Observed] = {for (i <- 1 to bigNumber) yield Observed(i , Random.nextInt)}.toDS()

  val detailsDataset: Dataset[Details] = {for (i <- 1 to bigNumber) yield Details(i , Random.nextInt)}.toDS()

  val joinedDetails: Dataset[JoinedDetails] = randomObservedDataset.joinSafe(detailsDataset,"left")(_.id,_.id)
    .map{case (observed,details) => JoinedDetails(observed.id,observed.count,details.tagNumber)}

  joinedDetails.show()
}


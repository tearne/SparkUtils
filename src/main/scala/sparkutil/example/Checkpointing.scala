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
import sampler.io.Logging
import sparkutil._

import scala.util.Random.nextDouble

/*
On first run a slow operation is executed and the results are saved to a cache file
On second run the results are loaded from the cache
 */
object Checkpointing extends App with Logging {
  val session: SparkSession = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

  import session.implicits._
  implicit val sesh = session

  val tmpFileDir = Paths.get("cache")

  case class Observed(id: Int, value: Double)

  info("start load data")
  val result: Dataset[Observed] = createCheckpoint(tmpFileDir.resolve(this.name)){
    (1 to 999).map{i =>
      Thread.sleep(10) // Slow operation
      Observed(i , nextDouble)
    }.toDS
  }
  info("finish load data")

  result.show(10)
}


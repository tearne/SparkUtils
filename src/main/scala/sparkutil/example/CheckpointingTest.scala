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
import sparkutil._

object CheckpointingTest extends App {
  val session: SparkSession = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

  import session.implicits._
  implicit val sesh = session
  val bigNumber = 99999

  val tmpFileDir = Paths.get("cache")
  val r = new scala.util.Random(100)

  case class Observed(id: Int, count: Int)

  def build(): Dataset[Observed] = {for (i <- 1 to bigNumber) yield Observed(i , Math.abs(r.nextInt()))}.toDS()

  createCheckpoint(tmpFileDir.resolve(this.name)){
    build()
  }

}


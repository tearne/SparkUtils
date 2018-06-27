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

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.{Dataset, SparkSession}
import sampler.io.Logging

/*

  Problem
  When handling records with timestamps, imprecision in the time can lead to
  ambiguity on the desired ordering.

  Solutoin
  1) Sort the records into a preferred order
  2) Zip with index
  3) Add small offsets to the timestamp to help keep the ordering deterministic

 */
object ZipAndPerturbDate extends App with Logging{
  val session: SparkSession = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

  import session.implicits._

  case class Record(date: Timestamp, data: Int)

  val today = LocalDate.now().atStartOfDay()
  val date1 = Timestamp.valueOf(today)
  val date2 = Timestamp.valueOf(today.plusDays(100))

  val raw: Dataset[Record] = (1 to 10)
      .map(i => if(i % 2 == 0) Record(date1, i) else Record(date2, i))
      .toDS


  info(">> Notice that there are repeated dates")
  info(">> Even if spark is always consistent in ordering, other applications might not be")
  raw.sortedBy(_.date).show

  val secondsInDay = 24 * 60 * 60
  def perturb(date: Timestamp, amount: Long) = {
    Timestamp.from(date.toInstant.plusSeconds(amount % secondsInDay))
  }

  info(">> Add a tiny offset to each timestamp, to inform unambiguous ordering")
  raw.sortedBy(_.date)
      .zipWithIndex(session)
      .map{case (record, idx) =>
        record.copy(date = perturb(record.date, idx))
      }.show

}

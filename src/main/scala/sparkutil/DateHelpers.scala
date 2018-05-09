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

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait DateHelpers {
  def parseToEpochDay(str: String)(implicit dateFormat: DateTimeFormatter): Long =
    LocalDateTime.parse(str, dateFormat).toLocalDate.toEpochDay

  def parseToEpochDay(ts: Timestamp): Long =
    ts.toLocalDateTime.toLocalDate.toEpochDay

  def getYear(str: String)(implicit dateFormat: DateTimeFormatter): Int =
    LocalDateTime.parse(str, dateFormat).toLocalDate.getYear

  def getYear(ts: Timestamp): Int = ts.toLocalDateTime.toLocalDate.getYear

  def daysBetween(start: Timestamp, end: Timestamp): Int = {
    val result = parseToEpochDay(end) - parseToEpochDay(start)
    assume(result >= 0)
    result.toInt
  }

}
object DateHelpers extends DateHelpers
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

import java.io.FileFilter
import java.nio.file.{Files, Path}
import java.time.LocalDate

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import sampler.io.Logging

import scala.util.Try

trait Checkpointing extends Logging {

  def createCheckpoint[T](
      parquetPath: Path,
      expireAfterDays: Int = 2
  )(
      dataBuilder: => Dataset[T] // "Call by name"
  )(
      implicit session: SparkSession,
      e: Encoder[T]
  ): Dataset[T] ={

    val today = LocalDate.now

    val fileName = parquetPath.getFileName
    val dir = parquetPath.getParent
    if(!Files.exists(dir)){
      info(s"Checkpoint dir does not exist (will create): "+dir)
      Files.createDirectories(dir)
    }

    val fileFilter: FileFilter = new WildcardFileFilter(s"checkpoint.$fileName.*")
    val candidateFiles = dir.toFile
        .listFiles(fileFilter)
        .map(_.getName)
        .map{name => (name, LocalDate.parse(name.splitAt(name.lastIndexOf(".")+1)._2)) }
        .sortBy{ case (_, date) => date.toEpochDay * -1}
        .collect{ case (name, date) if today.minusDays(expireAfterDays+1).isBefore(date) => name }

    def attemptLoad(file: Path): Dataset[T] = {
      Try(session.read.parquet(file.toString).as[T]).recover{ case _ =>
        warn("Exception reading parquet checkpoint.  Will delete and rebuild.")
        FileUtils.deleteDirectory(file.toFile)
        buildNewCheckpoint()
      }.get
    }

    def buildNewCheckpoint() = {
      val fileToCreate = dir.resolve(s"checkpoint.$fileName.$today")
      info("Creating checkpoint: "+fileToCreate.toAbsolutePath)
      val d = dataBuilder
      d.persist
      d.write.parquet(fileToCreate.toString)
      d
    }

    if(candidateFiles.length >= 1){
      val file = dir.resolve(candidateFiles.head)
      if(candidateFiles.length == 1) info("Loading checkpoint: "+file)
      else warn("Multiple checkpoints found, loading most recent: "+file)
      attemptLoad(file)
    }
    else {
      info("No checkpoint file found in time range.")
      buildNewCheckpoint()
    }
  }
}

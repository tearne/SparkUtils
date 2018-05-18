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

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StringType
import sampler.io.Logging

trait PersistenceHelpers extends Logging {

  implicit class RichString(str: String) {
    def toOption: Option[String] = Option(str).filterNot(_.trim.isEmpty())
  }

  implicit class ObjectName(obj: Any) {
    def name(): String = obj.getClass.getSimpleName.replace("$", "")
  }

  def distributedWriteLocalMerge(path: Path, dataset: Dataset[_]): Unit =
    distributedWriteLocalMerge(path, dataset, None)
  def distributedWriteLocalMerge(path: Path, dataset: Dataset[_], header: String): Unit =
    distributedWriteLocalMerge(path, dataset, Some(header))

  private def distributedWriteLocalMerge(path: Path, dataset: Dataset[_], headerOpt: Option[String]): Unit = {
    val partsPath = path.getParent.resolve(path.getFileName+".parts")

    if(dataset.schema.fields.length != 1 || dataset.schema.fields.head.dataType != StringType)
      error("Dataset schemea is not a single string: "+dataset.schema.fields.toSeq)

    if(Files.exists(partsPath)) {
      warn("Parts file already exists, deleting: "+partsPath)
      FileUtils.deleteDirectory(partsPath.toFile)
    }

    info("Writing parts files "+partsPath.toAbsolutePath.toString)
    dataset.write.text(partsPath.toAbsolutePath.toString)

    mergeParts(partsPath, path, headerOpt)
  }

  def mergeParts(partsPath: Path, mergedPath: Path, headerOpt: Option[String]): Unit = {
    info("Merging parts from "+partsPath)

    if(Files.exists(mergedPath)) {
      warn("... deleting existing merge file: "+mergedPath)
      Files.delete(mergedPath)
    }

    import scala.collection.JavaConverters._

    val files: Seq[Path] = Files
        .walk(partsPath)
        .iterator()
        .asScala
        .filterNot(_ == partsPath) // Don't include the directory itself
        .filterNot(Files.isHidden)  // Don't include hidden CRC files
        .toSeq
    assume(files.count(_.getFileName.toString == "_SUCCESS") == 1, "Missing _SUCCESS marker")
    assume(!files.exists(_.getFileName == "_temporary"), "_temporary marker found")
    assume(!files.exists(Files.isDirectory(_)), "Sub-directory found: " + files.filter(Files.isDirectory(_)).toIndexedSeq)

    val csvFiles: Seq[Path] = files.filterNot(_.getFileName == "_SUCCESS")
    assume(
      csvFiles.map { filePath =>
        val name = filePath.getFileName
        name.endsWith(".csv") && name.startsWith("part-")
      }.exists(!_),
      """Files not of expected format "part-*.csv""""
    )

    info(s"... parts files look fine")

    import java.nio.charset.StandardCharsets.UTF_8
    import java.nio.file.StandardOpenOption.{APPEND, CREATE}

    headerOpt.foreach{ header =>
      Files.write(mergedPath, Seq(header).asJava, UTF_8, CREATE)
      info(s"... wrote header ($header) to $mergedPath")
    }

    info("... start appending")
    csvFiles.foreach { inFile =>
      val lines = Files.readAllLines(inFile)
      Files.write(mergedPath, lines, UTF_8, APPEND, CREATE)
    }
    info(s"... done appending")
  }
}

object PersistenceHelpers extends PersistenceHelpers
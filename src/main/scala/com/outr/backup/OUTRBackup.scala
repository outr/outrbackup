package com.outr.backup

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import com.twitter.util.StorageUnit
import org.powerscala.event.Listenable
import org.powerscala.enum.{Enumerated, EnumEntry}
import org.powerscala.log.Logging
import scala.annotation.tailrec
import org.powerscala.concurrent.{AtomicInt, Executor, Time}
import org.powerscala.IO

/**
 * @author Matt Hicks <matt@outr.com>
 */
object OUTRBackup extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: OUTRBackup <origin directory> <destination directory>")
    } else {
      val origin = new File(args(0))
      val destination = new File(args(1))
      backup(origin, destination)
    }
  }

  def backup(origin: File, destination: File) = {
    val instance = new BackupInstance(origin.getCanonicalFile, destination.getCanonicalFile)
    info("\tIndexing changes...")
    val indexedIn = Time.elapsed {
      instance.index(threads = 8)
    }
    info(s"\tIndexed in $indexedIn seconds")
//    instance.dump()
    info("\tSynchronizing changes...")
    val syncIn = Time.elapsed {
      instance.sync()
    }
    info(s"\tSynchronized in $syncIn seconds")
    instance.dump()
  }
}

class BackupInstance(originDirectory: File, destinationDirectory: File) extends Listenable with Logging {
  val started = System.currentTimeMillis()
  val originPath = originDirectory.getAbsolutePath
  val destinationPath = destinationDirectory.getAbsolutePath

  val statFiles = new AtomicInt(0)
  val statDirectories = new AtomicInt(0)
  val statDirectoriesToCreate = new AtomicInt(0)
  val statDirectoriesToUpdate = new AtomicInt(0)
  val statDirectoriesToDelete = new AtomicInt(0)
  val statFilesToCreate = new AtomicInt(0)
  val statFilesToUpdate = new AtomicInt(0)
  val statFilesToDelete = new AtomicInt(0)
  val statDataToCopy = new AtomicLong(0L)
  val statDataCopied = new AtomicLong(0L)

  private val fileOperations = new ConcurrentLinkedQueue[FileOperation]()
  def add(operation: FileOperation) = {
    fileOperations.add(operation)
    if (operation.origin.isDirectory) {
      operation.operation match {
        case Operation.Create => statDirectoriesToCreate += 1
        case Operation.Update => statDirectoriesToUpdate += 1
        case Operation.Delete => statDirectoriesToDelete += 1
      }
    } else {
      operation.operation match {
        case Operation.Create => {
          statDataToCopy.addAndGet(operation.origin.length())
          statFilesToCreate += 1
        }
        case Operation.Update => {
          statDataToCopy.addAndGet(operation.origin.length())
          statFilesToUpdate += 1
        }
        case Operation.Delete => statFilesToDelete += 1
      }
    }
  }

  private val directoriesToIndex = new ConcurrentLinkedQueue[File]()

  directoriesToIndex.add(originDirectory)
  @volatile private var indexing = true
  private val currentlyIndexing = new AtomicInt(0)
  private val indexWorkersRunning = new AtomicInt(0)

  def index(threads: Int = 8) = {
    for (i <- 0 until threads) {
      addIndexWorker()
    }
    do {
      indexInternal()
    } while(currentlyIndexing() > 0)
    indexing = false
  }

  private def addIndexWorker() = Executor.invoke {
    indexWorkersRunning += 1
    while (indexing) {
      Time.sleep(0.5)
      indexInternal()
    }
    indexWorkersRunning -= 1
  }

  @tailrec
  private def indexInternal(): Unit = directoriesToIndex.poll() match {
    case null => // Nothing to do
    case origin => {
      currentlyIndexing += 1
      try {
        statDirectories += 1

        // Check the destination directory to make sure it exists
        val destination = origin2Destination(origin)
        if (!destination.exists()) {
          add(FileOperation(origin, destination, Operation.Create))
        } else {
          // Check the origin list of files to make sure they exist
          try {
            origin.listFiles().foreach {
              case o if o.isDirectory => directoriesToIndex.add(o)
              case o => {
                statFiles += 1
                val d = origin2Destination(o)
                if (!d.exists()) {
                  add(FileOperation(o, d, Operation.Create))
                } else if (d.lastModified() != o.lastModified() || d.length() != o.length()) {
                  add(FileOperation(o, d, Operation.Update))
                }
              }
            }
          } catch {
            case t: Throwable => throw new RuntimeException(s"Failed to process origin: ${origin.getName}.", t)
          }
          // Check the destination list of files to remove ones that don't exist on the origin
          destination.listFiles().foreach {
            case d => {
              val o = destination2Origin(d)
              if (!o.exists()) {
                add(FileOperation(o, d, Operation.Delete))
              }
            }
          }
        }
      } finally {
        currentlyIndexing -= 1
      }
      indexInternal()
    }
  }

  def dump() = {
    info(s"\tFiles Examined: ${statFiles()}, Directories Examined: ${statDirectories()}")
    info(s"\tDirectories - Create: ${statDirectoriesToCreate()}, Update: ${statDirectoriesToUpdate()}, Delete: ${statDirectoriesToDelete()}")
    info(s"\tFiles - Create: ${statFilesToCreate()}, Update: ${statFilesToUpdate()}, Delete: ${statFilesToDelete()}")
  }

  private val syncThreads = new AtomicInt(0)

  def sync(threads: Int = 2) = {
    (0 until threads).foreach(index => syncThread())

    while (syncThreads() > 0) {
      Time.sleep(0.5)
    }
  }

  private def syncThread() = {
    syncThreads += 1
    Executor.invoke {
      try {
        syncInternal()
      } finally {
        syncThreads -= 1
      }
    }
  }

  @tailrec
  private def syncInternal(): Unit = fileOperations.poll() match {
    case null => // Nothing left to do
    case fo => {
      // TODO: output total data to copy, current data progress, time taken for current, remove origin, size of current
      try {
        val copied = new StorageUnit(statDataCopied.get()).toHuman()
        val toCopy = new StorageUnit(statDataToCopy.get()).toHuman()
        val elapsed = Time.elapsed(System.currentTimeMillis() - started).shorthand
        fo.operation match {
          case Operation.Create => if (fo.origin.isDirectory) {
            fo.destination.mkdirs()
            fo.origin.listFiles().foreach {
              case o => add(FileOperation(o, origin2Destination(o), Operation.Create))
            }
          } else {
            print(s"* Create: ${fo.destination.getAbsolutePath} (${new StorageUnit(fo.origin.length()).toHuman()}) [$copied of $toCopy] ... ")
            val time = Time.elapsed {
              IO.copy(fo.origin, fo.destination)
            }
            statDataCopied.addAndGet(fo.origin.length())
            fo.destination.setLastModified(fo.origin.lastModified())
            info(s"${Time.elapsed(time).shorthand}, Elapsed: $elapsed")
          }
          case Operation.Update => {
            print(s"* Update: ${fo.destination.getAbsolutePath} (${new StorageUnit(fo.origin.length()).toHuman()}) [$copied of $toCopy] ... ")
            val time = Time.elapsed {
              IO.copy(fo.origin, fo.destination)
            }
            statDataCopied.addAndGet(fo.origin.length())
            fo.destination.setLastModified(fo.origin.lastModified())
            info(s"${Time.elapsed(time).shorthand}, Elapsed: $elapsed")
          }
          case Operation.Delete => {
            print(s"* Delete: ${fo.destination.getAbsolutePath} (${new StorageUnit(fo.origin.length()).toHuman()}) [$copied of $toCopy] ... ")
            val time = Time.elapsed {
              IO.delete(fo.destination)
            }
            info(s"${Time.elapsed(time).shorthand}, Elapsed: $elapsed")
          }
        }
      } catch {
        case t: Throwable => throw new RuntimeException(s"Failed to ${fo.operation} ${fo.origin.getName} to ${fo.destination.getName}.", t)
      }
      syncInternal()
    }
  }

  def origin2Destination(file: File) = {
    val filePath = file.getAbsolutePath
    val relativePath = if (filePath.length != originPath.length) {
      filePath.substring(originPath.length + 1)
    } else {
      ""
    }
    new File(destinationDirectory, relativePath)
  }

  def destination2Origin(file: File) = {
    val filePath = file.getAbsolutePath
    val relativePath = filePath.substring(destinationPath.length + 1)
    new File(originDirectory, relativePath)
  }
}

case class FileOperation(origin: File, destination: File, operation: Operation)

sealed abstract class Operation extends EnumEntry

object Operation extends Enumerated[Operation] {
  case object Create extends Operation
  case object Update extends Operation
  case object Delete extends Operation

  val values = findValues.toVector
}
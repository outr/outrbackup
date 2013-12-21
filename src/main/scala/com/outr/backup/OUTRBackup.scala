package com.outr.backup

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import org.powerscala.event.Listenable
import org.powerscala.enum.{Enumerated, EnumEntry}
import scala.annotation.tailrec
import org.powerscala.concurrent.{AtomicInt, Executor, Time}
import org.powerscala.IO

/**
 * @author Matt Hicks <matt@outr.com>
 */
object OUTRBackup {
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
    println("\tIndexing changes...")
    val indexedIn = Time.elapsed {
      instance.index(threads = 8)
    }
    println(s"\tIndexed in $indexedIn seconds")
//    instance.dump()
    println("\tSynchronizing changes...")
    val syncIn = Time.elapsed {
      instance.sync()
    }
    println(s"\tSynchronized in $syncIn seconds")
    instance.dump()
  }
}

class BackupInstance(originDirectory: File, destinationDirectory: File) extends Listenable {
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
        case Operation.Create => statFilesToCreate += 1
        case Operation.Update => statFilesToUpdate += 1
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
          origin.listFiles().foreach {
            case o if o.isDirectory => directoriesToIndex.add(o)
            case o => {
              statFiles += 1
              val d = origin2Destination(o)
              if (!d.exists()) {
                add(FileOperation(o, d, Operation.Create))
              } else if (d.lastModified() != o.lastModified()) {
                add(FileOperation(o, d, Operation.Update))
              }
            }
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
    println(s"\tFiles Examined: ${statFiles()}, Directories Examined: ${statDirectories()}")
    println(s"\tDirectories - Create: ${statDirectoriesToCreate()}, Update: ${statDirectoriesToUpdate()}, Delete: ${statDirectoriesToDelete()}")
    println(s"\tFiles - Create: ${statFilesToCreate()}, Update: ${statFilesToUpdate()}, Delete: ${statFilesToDelete()}")
  }

  @tailrec
  final def sync(): Unit = fileOperations.poll() match {
    case null => // Nothing left to do
    case fo => {
      fo.operation match {
        case Operation.Create => if (fo.origin.isDirectory) {
          fo.destination.mkdirs()
          fo.origin.listFiles().foreach {
            case o => add(FileOperation(o, origin2Destination(o), Operation.Create))
          }
        } else {
          print(s"\t\t* Create: ${fo.origin.getAbsolutePath} to ${fo.destination.getAbsolutePath} ... ")
          IO.copy(fo.origin, fo.destination)
          fo.destination.setLastModified(fo.origin.lastModified())
          println("COMPLETE")
        }
        case Operation.Update => {
          print(s"\t\t* Update: ${fo.origin.getAbsolutePath} to ${fo.destination.getAbsolutePath} ... ")
          IO.copy(fo.origin, fo.destination)
          fo.destination.setLastModified(fo.origin.lastModified())
          println("COMPLETE")
        }
        case Operation.Delete => {
          print(s"\t\t* Delete: ${fo.destination.getAbsolutePath} ... ")
          IO.delete(fo.destination)
          println("COMPLETE")
        }
      }
      sync()
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

class Operation private() extends EnumEntry

object Operation extends Enumerated[Operation] {
  val Create = new Operation
  val Update = new Operation
  val Delete = new Operation
}
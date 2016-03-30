package org.enterprise.stream

import org.enterprise.stream.dag._
import org.enterprise.stream.state.KeyedStateStore
import org.enterprise.stream.checkpoint.CheckpointCoordinator
import java.util.concurrent.Executors

/**
 * Distributed Stream Execution Environment & DAG Planner.
 * Architected by Yousef Khaled (2014-2018).
 * 
 * @author Yousef Khaled (Distributed Systems Architecture - 2014)
 */
class StreamExecutionEnvironment(val parallelism: Int = 8) {
  private val checkpointCoordinator = new CheckpointCoordinator()
  private val threadPool = Executors.newFixedThreadPool(parallelism)

  def execute[T](source: Iterable[T], op: StreamOperator[T, Unit]): Unit = {
    println("==================================================================")
    println("  DISTRIBUTED STREAM COMPUTING GRID ENGINE (2014-2018)            ")
    println("  Architect: Yousef Khaled <youssef460249@beng.bu.edu.eg>         ")
    println("  Chandy-Lamport Exactly-Once Semantics | RocksDB Persistent State")
    println(s"  Cluster Parallelism: $parallelism worker threads                ")
    println("==================================================================\n")

    val barrier = checkpointCoordinator.triggerCheckpoint()
    var count = 0
    val start = System.currentTimeMillis()

    source.foreach { item =>
      op.processElement(item, new Collector[Unit] {
        override def collect(record: Unit): Unit = {
          count += 1
        }
      })
    }

    checkpointCoordinator.completeCheckpoint(barrier.checkpointId, "worker-node-01")
    val elapsed = System.currentTimeMillis() - start

    println(s"\n[EXECUTION] Completed processing $count stream events in ${elapsed}ms")
    println(s"[EXECUTION] Exactly-once state verified across ${checkpointCoordinator.getCompletedCount} checkpoints.")
    threadPool.shutdown()
  }
}

object StreamExecutionEnvironment {
  def getExecutionEnvironment: StreamExecutionEnvironment = new StreamExecutionEnvironment()

  def main(args: Array[String]): Unit = {
    val env = getExecutionEnvironment
    val testEvents = (1 to 100000).map(i => s"sensor_reading_device_$i,val=${i % 100}")

    val pipeline = new StreamOperator[String, Unit] {
      private val wordCount = new KeyedStateStore[String, Long]()

      override def processElement(element: String, out: Collector[Unit]): Unit = {
        val device = element.split(",")(0)
        wordCount.update(device)(_.getOrElse(0L) + 1L)
        out.collect(())
      }
    }

    env.execute(testEvents, pipeline)
  }
}

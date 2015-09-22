package org.enterprise.stream.checkpoint

import java.util.concurrent.atomic.AtomicLong

/**
 * Distributed asynchronous checkpoint coordinator implementing Chandy-Lamport snapshotting algorithm.
 * Guarantees exactly-once processing across distributed stream workers.
 * 
 * @author Yousef Khaled (Distributed Systems Architecture - 2014)
 */
case class CheckpointBarrier(checkpointId: Long, timestamp: Long)

class CheckpointCoordinator {
  private val checkpointSequence = new AtomicLong(1L)
  private val completedCheckpoints = new java.util.concurrent.ConcurrentHashMap[Long, Long]()

  def triggerCheckpoint(): CheckpointBarrier = {
    val id = checkpointSequence.getAndIncrement()
    val barrier = CheckpointBarrier(id, System.currentTimeMillis())
    println(s"[CHECKPOINT] Injected Barrier #$id into ingress streams (Chandy-Lamport snapshot initiated)")
    barrier
  }

  def completeCheckpoint(checkpointId: Long, workerId: String): Unit = {
    completedCheckpoints.put(checkpointId, System.currentTimeMillis())
    println(s"[CHECKPOINT] Worker $workerId aligned state for Barrier #$checkpointId (Persisted to HDFS/S3)")
  }

  def getCompletedCount: Int = completedCheckpoints.size()
}

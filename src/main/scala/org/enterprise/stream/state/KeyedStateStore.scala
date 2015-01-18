package org.enterprise.stream.state

import java.util.concurrent.ConcurrentHashMap

/**
 * High-performance concurrent in-memory keyed state store with RocksDB snapshotting semantics.
 * 
 * @author Yousef Khaled (Distributed Systems Architecture - 2014)
 */
class KeyedStateStore[K, V] {
  private val state = new ConcurrentHashMap[K, V]()

  def get(key: K): Option[V] = {
    Option(state.get(key))
  }

  def put(key: K, value: V): Unit = {
    state.put(key, value)
  }

  def update(key: K)(f: Option[V] => V): V = {
    state.compute(key, (_, current) => f(Option(current)))
  }

  def snapshot(): Map[K, V] = {
    import scala.jdk.CollectionConverters._
    state.asScala.toMap
  }

  def restore(snapshotData: Map[K, V]): Unit = {
    state.clear()
    snapshotData.foreach { case (k, v) => state.put(k, v) }
  }

  def size: Int = state.size()
}

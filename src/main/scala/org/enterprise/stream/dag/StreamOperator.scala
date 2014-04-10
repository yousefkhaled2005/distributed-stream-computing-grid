package org.enterprise.stream.dag

/**
 * Functional stream operator interface for distributed DAG computation.
 * 
 * @author Yousef Khaled (Distributed Systems Architecture - 2014)
 */
trait StreamOperator[IN, OUT] extends Serializable {
  def processElement(element: IN, out: Collector[OUT]): Unit
  def open(): Unit = {}
  def close(): Unit = {}
}

trait Collector[T] {
  def collect(record: T): Unit
}

class MapOperator[IN, OUT](mapper: IN => OUT) extends StreamOperator[IN, OUT] {
  override def processElement(element: IN, out: Collector[OUT]): Unit = {
    out.collect(mapper(element))
  }
}

class FilterOperator[T](predicate: T => Boolean) extends StreamOperator[T, T] {
  override def processElement(element: T, out: Collector[T]): Unit = {
    if (predicate(element)) {
      out.collect(element)
    }
  }
}

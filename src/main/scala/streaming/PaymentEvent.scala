package src.main.scala.streaming

import java.sql.Timestamp
import scala.collection.immutable.Queue

case class PaymentEvent(id: String, issuer: String, event_name: String, merchant_id: String , timestamp: Timestamp)

case class PaymentEventCount(key: String,
                             startTime: Timestamp,
                             endTime: Timestamp,
                             success: Double,
                             failure: Double)

case class FIFOBuffer[T](capacity: Int, data: Queue[T] = Queue.empty) extends Serializable {
  def add(element: T): FIFOBuffer[T] = this.copy(data = data.enqueue(element).takeRight(capacity))
  def get: List[T] = data.toList
  def size: Int = data.size
}

package streaming


import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import src.main.scala.streaming.{FIFOBuffer, PaymentEvent, PaymentEventCount}
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.functions.expr


object StructuredStreaming {
  val ElementCountWindowSize = 10

  def stateToAverageEvent(key: (String, String), data: FIFOBuffer[PaymentEvent]):Iterator[PaymentEventCount] = {
    if (data.size == ElementCountWindowSize) {
      val events = data.get
      val start = events.head
      val end = events.last
      var success = 0
      var failure = 0
      for (event <- events) {
        if (event.event_name.equals("PAYMENT.VERIFICATION.INITIATED")) {
          success = success + 1
        }
        if (event.event_name.equals("PAYMENT.AUTHORIZATION.COMPLETED")) {
          failure = failure + 1
        }
      }
      Iterator(PaymentEventCount(key.toString(), start.timestamp, end.timestamp, success, failure))
    } else {
      Iterator.empty
    }
  }

  def flatMappingFunction(key: (String, String), values: Iterator[PaymentEvent], state: GroupState[FIFOBuffer[PaymentEvent]]):
  Iterator[PaymentEventCount] = {
    if (state.hasTimedOut) {
      val result = stateToAverageEvent(key, state.get)
      // evict the timed-out state
      state.remove()
      // emit the result of transforming the current state into an output record
      result
    } else {
      // get current state or create a new one if there's no previous state
      val currentState = state.getOption.getOrElse(new FIFOBuffer[PaymentEvent](ElementCountWindowSize))
      // enrich the state with the new events
      val updatedState = values.foldLeft(currentState){case (st, ev) => st.add(ev)}
      // update the state with the enriched state
      state.update(updatedState)
      state.setTimeoutDuration("30 seconds")
      // only when we have enough data, create a WeatherEventAverage from the accumulated state
      // before that, we return an empty result.
      stateToAverageEvent(key, updatedState)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkSession  = SparkSession.builder()
                            .appName("Spark Structured Streaming")
                            .master("local[*]")
                            .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")


    // schema of events received from input source.
    val schema = new StructType()
      .add("event_name", StringType)
      .add("producer_timestamp", TimestampType)
      .add("properties",
        new StructType().add("payment",
                                  new StructType()
                                    .add("id", StringType)
                                    .add("method", StringType)
                                    .add("issuer", StringType))
                        .add("merchant",
                                  new StructType()
                                    .add("id", StringType))
      )

    val streamingData = sparkSession
      .readStream
      .format("json")
      .option("maxFilesPerTrigger", 1)
      .schema(schema)
      .load(System.getProperty("user.dir") + "/json")

    import sparkSession.implicits._
    val paymentEventsStream = streamingData
                            .select($"properties.payment.id" as "id",
                              $"properties.payment.issuer" as "issuer",
                              $"properties.merchant.id" as "merchant_id",
                              $"producer_timestamp" as "timestamp",
                              $"event_name" as "event_name")
                            .as[PaymentEvent]


    val asd = paymentEventsStream.withWatermark("timestamp", "2 minutes")
                            .groupByKey(p => (p.issuer, p.merchant_id))
                            .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout)(flatMappingFunction)

    val query = asd.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    query.awaitTermination()
  }
}

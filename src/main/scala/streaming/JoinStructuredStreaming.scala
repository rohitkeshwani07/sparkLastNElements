package src.main.scala.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object JoinStructuredStreaming {
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
      .load(System.getProperty("user.dir") + "/join_json")

    import sparkSession.implicits._
    val paymentEventsStream = streamingData
                            .select($"properties.payment.id" as "id",
                              $"properties.payment.issuer" as "issuer",
                              $"properties.merchant.id" as "merchant_id",
                              $"producer_timestamp" as "timestamp",
                              $"event_name" as "event_name")
                            .as[PaymentEvent]


    val asdas = paymentEventsStream
      .filter("event_name = 'PAYMENT.VERIFICATION.INITIATED'")

    val asdas1 = paymentEventsStream
      .filter("event_name = 'PAYMENT.VERIFICATION.INITIATED1'")
      .withWatermark("timestamp", "2 minutes")

    val asdas2 = asdas.as("as").join(
      asdas1.as("as1"),
      expr("""
    as.id = as1.id AND
    as1.timestamp >= as.timestamp AND
    as1.timestamp <= as.timestamp + interval 1 minutes
    """), "LeftOuter"
    )

    val query = asdas2.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }
}

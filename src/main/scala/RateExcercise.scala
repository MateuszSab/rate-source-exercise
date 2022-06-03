import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger


//bin/spark-submit /mnt/c/RateSourceExcercise/target/scala-2.12/ratesourceexcercise_2.12-0.1.0-SNAPSHOT.jar /mnt/c/RateSourceExcercise/checklocationpatch/ /mnt/c/RateSourceExcercise/output

object RateExcercise extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("BRR")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._


  val checklocationpath = if (args.length > 0) args(0) else "checklocationpatch"
  val output = if (args.length > 1) args(1) else "output"

  val rates = spark
    .readStream
    .format("rate")
    .option("header", true)
    .load
    .as[(Timestamp, Long)]

  import scala.concurrent.duration._
  import java.io._

  var pw: PrintWriter = _

  rates.writeStream
    .format("console")
    .queryName("tabaluga")
    .trigger(Trigger.ProcessingTime(5.seconds))
    .option("checkpointLocation", checklocationpath)
    .foreach(new ForeachWriter[(Timestamp, Long)] {
      override def open(partitionId: Timestamp, epochId: Long): Boolean = {
        println(s"open >>> $partitionId, $epochId")
        try {
          pw = new PrintWriter(new File(output + s"/batch_${partitionId}_${epochId}"))
          true
        } catch {
          case _ =>
            println("błąd")
            false
        }
      }

      override def process(value: (Timestamp, Long)): Unit = {
        println(s"process >>> $value")
        pw.write(s"timestamp: ${value._1} => value: ${value._2}\n")
      }

      override def close(errorOrNull: Throwable): Unit = {
        println(s"close >>> $errorOrNull")
        pw.close
      }
    }).start().awaitTermination()
}

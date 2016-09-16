import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
//import kmeans.KMeansObj

object TrafficDataStreaming {
  def main(args: Array[String]) {

    val brokers = "ec2-23-22-195-205.compute-1.amazonaws.com:9092"
    val topics = "traffic_data"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("traffic_data")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val msgDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Create KMeans object
    //    val kmo = new KMeansObj

    // Iterate over DStream to get incoming traffic
    msgDStream.foreachRDD { rdd =>

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val lines = rdd.map(_._2)

      val ticksDF = lines.map( x => {
          val spl = x.split(',')
          val len = spl.length
          val buf = spl.toBuffer
          buf.remove(1)
          buf.remove(1)
          buf.remove(1)
          List("[", buf.toArray.mkString(","), "]").mkString("")
        })
        Tick(record)}).toDF()

      ticksDF.show()
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Tick(content: String)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
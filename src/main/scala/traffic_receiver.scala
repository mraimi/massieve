import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import java.io._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TrafficDataStreaming {
  def main(args: Array[String]) {

    val brokers = "ec2-23-22-195-205.compute-1.amazonaws.com:9092"
    val topics = "traffic_data"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("traffic_data")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Iterate over DStream to get incoming traffic
    inputDStream.foreachRDD( rdd => {

      val lines = rdd.map(_._2)
      lines.map( rec => {
        val spl = rec.split(',')
        val len = spl.length
        val buf = spl.toBuffer
        buf.remove(1, 3)
        buf.toArray.mkString(",")
      })

      lines.saveAsTextFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/test/data-", lines.id).mkString(""))

    })

    /**
      * KMeans code follows
      */

    val trainingData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/train/").map(Vectors.parse)
    val testData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/test/").map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(100)
      .setDecayFactor(0.0)
      .setRandomCenters(38)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}

import kafka.serializer.StringDecoder
import java.io.Serializable
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import java.io._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.clustering.StreamingKMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.redis._
import com.typesafe.config.ConfigFactory


/** Object for distance mesaurements and cluster membership */
case object DistanceFunctions extends Serializable {

  /**
    * Standard implementation of Euclidian distance between
    * two vectors of the same dimension
    */
  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d*d).sum)

  /**
    * Takes raw traffic records, strips non-numerical fields,
    * and casts numbers to doubles. Returns records of the form:
    * (Vector[Double], Double, (String, String, String)) representing
    * the record, its distance from its assigned centroid, and
    * a tuple of its connection and protocol.
    */
  def distToCentroid(data: RDD[String], model: StreamingKMeansModel) = {
    val clusters = data.map(rec => {
        val buf = rec.split(',').toBuffer
        val removed = (buf(1), buf(2))
        val cruft = buf.remove(1,3)
        val dubs = buf.toArray.map(_.toDouble)
        val record = Vectors.dense(dubs)
        (record, model.predict(record), removed)
    })
    clusters.map(tup => (tup._2, distance(tup._1, model.clusterCenters(tup._2)), tup._3))
  }

  /**
    * Reads initial thresholds built on historical data
    */
  def getThresholds(stats: RDD[String], std_dev_multiplier: Double) = {
    stats.map(line => {
      val spl = line.split(",")
      val dub = spl.map(_.toDouble)
      (dub(0), dub(1)+std_dev_multiplier*dub(2))
    }).collectAsMap
  }
}

/** Serializable singleton RedisClient to be distributed */
object RedisConnection extends Serializable {
  lazy val client: RedisClient = new RedisClient("ec2-52-54-82-137.compute-1.amazonaws.com", 6379, 0, Option("BFHW6zDv3g7kuxDxRXV7K8Y2pdyfR7kw"))
}

object TrafficDataStreaming {
  def main(args: Array[String]) {
    
    val df = DistanceFunctions
    val baseUrl = "hdfs://ec2-23-22-195-205.compute-1.amazonaws.com"
    val brokers = ":9092"
    val topics = "traffic_data4"
    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("traffic_data")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sc = ssc.sparkContext
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val inputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val trainingData = ssc.textFileStream(baseUrl + ":9000/train/").map(Vectors.parse)
    val statsTextFile = sc.textFile(baseUrl + ":9000/stats")
    val thresholds = df.getThresholds(statsTextFile, 0.0)
    val bcThresh = sc.broadcast(thresholds)
    val model = new StreamingKMeans().setK(100).setDecayFactor(0.0).setRandomCenters(38, 0.0)

    model.trainOn(trainingData)
    val latest = sc.broadcast(model.latestModel)

    /** Iterate over new traffic records */
    inputStream.foreachRDD(rdd => {
      val distRdd = df.distToCentroid(rdd.map(_._2), latest.value)
      val results = distRdd.map(distanceTup => {
        val clusterIdx = distanceTup._1
        val dist = distanceTup._2
        val present = bcThresh.value.contains(clusterIdx.toDouble)
        val exceedsThreshold = present && dist > bcThresh.value(clusterIdx.toDouble)

        /** Check if distance for current record exceeds the threshold */
        val result = if (exceedsThreshold) "anomalous" else "normal";

        /** Publish the result to Redis **/
        val channel = List(distanceTup._3._1, distanceTup._3._2).mkString(".")
        val message = List(distanceTup._3._1, distanceTup._3._2, result).mkString(",")
        RedisConnection.client.publish(channel, message)
        (result, distanceTup._3)
      })

      if (!results.isEmpty){
        /** Write a copy to HDFS for cold storage */
        results.saveAsTextFile(List(baseUrl + ":9000/output/traffic-results-", distRdd.id).mkString(""))
      }


      if (!rdd.isEmpty){
        /** Write back to model */
        rdd.saveAsTextFile(List(baseUrl + ":9000/train/to-train-", distRdd.id).mkString(""))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

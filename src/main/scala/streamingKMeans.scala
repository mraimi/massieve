import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.clustering.StreamingKMeansModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.Map

object StreamingKMeansExample {

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d*d).sum)

  /**
    * @return Rdd[(Double, Double)]
    */
  def distToCentroid(data: RDD[Vector], model: StreamingKMeansModel) = {
    val clusters = data.map(record => (record, model.predict(record)))
    clusters.map(tup => (tup._2, distance(tup._1, model.clusterCenters(tup._2))))
  }

  def getThresholds(stats: RDD[String], std_dev_multiplier: Double) = {
    stats.map(line => {
      val spl = line.split(",")
      val dub = spl.map(_.toDouble)
      (dub(0), dub(1)+std_dev_multiplier*dub(2))
    }).collectAsMap
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("kmeans")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sc = ssc.sparkContext

    val trainingData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/train/").map(Vectors.parse)
    val testData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/test/").map(Vectors.parse)
    val statsTextFile = sc.textFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/stats")
    val thresholds = getThresholds(statsTextFile, 1.0)
    val bcThresh = sc.broadcast(thresholds)

    val model = new StreamingKMeans().setK(100).setDecayFactor(0.0).setRandomCenters(38, 0.0)

    model.trainOn(trainingData)
    val latest = sc.broadcast(model.latestModel)

    testData.foreachRDD(rdd => {
      val distRdd = distToCentroid(rdd, latest.value)
      val results = distRdd.map(distanceTup => {
        val idx = distanceTup._1
        val dist = distanceTup._2
        if (bcThresh.value.contains(idx.toDouble) && dist > bcThresh.value(idx.toDouble)) "Normal" else "Anomalous"
      })

      if (!distRdd.isEmpty){
        distRdd.saveAsTextFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/distance-", distRdd.id).mkString(""))
      }

      if (!results.isEmpty){
        results.saveAsTextFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/traffic-results-", distRdd.id).mkString(""))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

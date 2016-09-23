import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.clustering.StreamingKMeansModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, Streaming Context}
import scala.collection.Map

object StreamingKMeansExample {

  def getDistances(inputDStream: DStream[String]) = {
    inputDStream.forEachRDD(rdd => {
      val vectorizedRdd = rdd.map(record => {
        val buffer record.split(",").toBuffer.remove(1,3)
        Vectors.dense(buffer.map(_._1.toDouble).toArray)
      })
    })
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d*d).sum)

  def distToCentroid(data: RDD[Vector], model: StreamingKMeansModel) = {
    val clusters = data.map(record => (record, model.predict(record)))
    clusters.map(tup => (tup._1, distance(tup._1, model.clusterCenters(tup._2))))
  }

  def getThresholds(sc: SparkContext, std_dev_multiplier: Double) = {
    val thresh = sc.textFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/threshold")
    val tups = thresh.map(line => {
      val spl = line.split(",")
      val dub = spl.map(_.toDouble)
      (dub(0), dub(1)+std_dev_multiplier*dub(2))
    }).collectAsMap
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("kmeans")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val thresholds = getThresholds(scc.sparkContext)

    val trainingData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/train/").map(Vectors.parse)
    val testData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/test/").map(Vectors.parse)

    val model = new StreamingKMeans().setK(100).setDecayFactor(0.0).setRandomCenters(38, 0.0)

    model.trainOn(trainingData)
    testData.foreachRDD(rdd => {
      val distRdd = distToCentroid(rdd, model)
      val results = distRdd.map(distanceTup => {
        val idx = distanceTup._1
        val dist = distanceTup._2


      })

      if (!distRdd.isEmpty){
        /** Write back to the model for updates */
        rdd.saveAsTextFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/distance-", rdd.id).mkString(""))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

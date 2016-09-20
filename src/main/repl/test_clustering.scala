import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

val ssc = new StreamingContext(sc, Seconds(5))

val trainingData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/unlabeled_full_numeric").map(Vectors.parse)
val testData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/test/labeled_full_numeric").map(LabeledPoint.parse)

val model = new StreamingKMeans().setK(100).setDecayFactor(0).setRandomCenters(38, 0.0)

model.trainOn(trainingData)
model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

ssc.start()
ssc.awaitTermination()
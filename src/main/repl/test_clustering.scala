import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

val ssc = new StreamingContext(sc, Seconds(30))
val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)

// Get training data into appropriate form

val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

val model = new StreamingKMeans()
  .setK(100)
  .setDecayFactor(0)
  .setRandomCenters(38, 0.0)
  .trainOn

model.trainOn(trainingData)
model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

ssc.start()
ssc.awaitTermination()
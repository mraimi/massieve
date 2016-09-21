/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

import org.apache.spark.SparkConf
// $example on$
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
// $example off$

/**
  * Estimate clusters on one stream of data and make predictions
  * on another stream, where the data streams arrive as text files
  * into two different directories.
  *
  * The rows of the training text files must be vector data in the form
  * `[x1,x2,x3,...,xn]`
  * Where n is the number of dimensions.
  *
  * The rows of the test text files must be labeled data in the form
  * `(y,[x1,x2,x3,...,xn])`
  * Where y is some identifier. n must be the same for train and test.
  *
  * Usage:
  *   StreamingKMeansExample <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
  *
  * To run on your local machine using the two directories `trainingDir` and `testDir`,
  * with updates every 5 seconds, 2 dimensions per data point, and 3 clusters, call:
  *    $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
  *
  * As you add text files to `trainingDir` the clusters will continuously update.
  * Anytime you add text files to `testDir`, you'll see predicted labels using the current model.
  *
  */
object StreamingKMeansExample {

  def main(args: Array[String]) {

    val trainingData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/train/").map(Vectors.parse)
    val testData = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/test/").map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(100)
      .setDecayFactor(0.0)
      .setRandomCenters(38)

    model.trainOn(trainingData)
    val outputDStream = model.predictOn(testData)
    outputDStream.print()
    outputDStream.foreachRDD(rdd => {
      val distRdd = distToCentroid(rdd, model)
      
      if (!rdd.isEmpty){
        /** Write back to the model for updates */
        rdd.saveAsTextFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/predict-", rdd.id).mkString(""))
      }

      if (!distRdd.isEmpty){
        /** Write back to the model for updates */
        rdd.saveAsTextFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/distance-", rdd.id).mkString(""))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

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
    /** @val clusters RDD[Int] Centroid indices for each record **/
    val clusters = data.map(record => (record, model.predict(record)))
    clusters.map(tup => (tup._1, distance(tup._1, model.clusterCenters(tup._2))))
  }
}
// scalastyle:on println

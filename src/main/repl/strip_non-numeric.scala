import org.apache.spark.mllib.linalg._

val rawData = sc.textFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/kddcup.data")

val labelsAndData = rawData.map(line => {
  val buffer = line.split(',').toBuffer
  buffer.remove(1,3)
  val label = buffer.remove(buffer.length-1)
  val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
  (label,vector)
})

labelsAndData.saveAsTextFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/labeled_full_numeric")

val labeledTestNumeric = labelsAndData.sample(false, .30)

labeledTestNumeric.saveAsTextFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/labeled_test_numeric")



val numericData = rawData.map(line => {
  val buffer = line.split(',').toBuffer
  buffer.remove(1,3)
  buffer.remove(buffer.length-1)
  Vectors.dense(buffer.map(_.toDouble).toArray)
})

numericData.saveAsTextFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/unlabeled_full_numeric")

val testData = sc.textfile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/kddcup.testdata.unlabeled")

val testNumeric = testData.map(line => {
  val buf = line.split(",").toBuffer
  buf.remove(1,3)
  Vectors.dense(buf.map(_.toDouble).toArray)
})

testNumeric.saveAsTextFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data/unlabeled_test_numeric")
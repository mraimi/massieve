import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

val ssc = new StreamingContext(sc, Seconds(2))

val inputDStream = ssc.textFileStream("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/data")

inputDStream.foreachRDD( lines => {
  lines.map( rec => {
    val spl = rec.split(',')
    val len = spl.length
    val buf = spl.toBuffer
    buf.remove(1)
    buf.remove(1)
    buf.remove(1)
    buf.toArray.mkString(",")
  })
})

inputDStream.saveAsTextFiles("test")

ssc.start()
ssc.awaitTermination()
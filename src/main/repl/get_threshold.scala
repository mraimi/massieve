val data = sc.textFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/idx_dist-305")

val parsed = data.map(line => {
  val spl = line.split(",")
  (spl(0), Vector(spl(1).toDouble))
})

val red = parsed.reduceByKey((x,y) => x ++ y)
val results = red.map(tup => {
  val count = tup._2.length
  val mean = tup._2.sum / count
  val devs = tup._2.map(score => (score - mean) * (score - mean))
  val stddev = Math.sqrt(devs.sum / count)
  (tup._1, mean, stddev)
})

results.saveAsTextFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/stats")

//val mapVal = parsed.mapValues(x => (x,1))
//val cts = mapVal2.reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).map(tup => List().mkString(","))
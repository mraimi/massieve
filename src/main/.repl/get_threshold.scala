val data = sc.textFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/idx_dist-305")

val parsed = data.map(line => {
  val spl = line.split(",")
  (spl(0), Vector(spl(1).toDouble))
})

val red = parsed.reduceByKey((x,y) => x ++ y)
val results = red.map(tup => {
  val mean = tup._2.sum / tup._2.length
  val exp2 = tup._2.map(num => {
    Math.pow(num-mean,2)
  }).sum
  List(tup._1, mean, Math.sqrt(exp2/tup._2.length)).mkString(",")
})

results.saveAsTextFile("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/stats")
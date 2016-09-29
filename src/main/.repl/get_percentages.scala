val name = "traffic-results-42"
val data = sc.textFile(List("hdfs://ec2-23-22-195-205.compute-1.amazonaws.com:9000/output/", name).mkString(""))
val ct = data.count
val results = data.map(res => (res, 1)).reduceByKey((x, y) => x + y)
val percentages = results.map(res => (res._1, res._2.toDouble/ct.toDouble))

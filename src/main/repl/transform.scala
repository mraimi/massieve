val training = sc.textFile("/home/ubuntu/opt/realtimeAnomalies/src/main/training/kddcup.data")
val sub_sample = training.sample(false, .00001)
val tuples = sub_sample.map(line => {
  val spl = line.split(',')
  val len = spl.length
  val y = spl(len - 1)
  val buf = spl.toBuffer
  buf.remove(1)
  buf.remove(1)
  buf.remove(1)
  buf.remove(len - 4)
  (y, buf.toArray).toString()
})


val tuples = sub_sample.map(line => {
  line.split(',')
})
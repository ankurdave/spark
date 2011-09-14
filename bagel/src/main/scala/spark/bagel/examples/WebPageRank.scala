package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

object WebPageRank {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WebPageRank <inputFile> <threshold> <numSplits> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.PRSerializer")
    System.setProperty("spark.kryo.registrator", classOf[WGKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WebPageRank")

    val vertices = sc.objectFile[(Long, PRVertex)](inputFile, numSplits).cache

    println("Counting vertices...")
    val numVertices = vertices.count()
    println("Done counting vertices: " + numVertices)

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(Long, PRMessage)]())
    val util = new PageRankUtils
    val partitioner =
      if (usePartitioner) new CustomPartitioner(numSplits)
      else new HashPartitioner(numSplits)
    val result =
      Bagel.run(
        sc, vertices, messages, combiner = new PRCombiner(),
        partitioner = partitioner, numSplits = numSplits)(
        util.computeWithCombiner(numVertices, epsilon))

    // Print the result
    System.err.println("Pages with PageRank >= " + threshold + ":")
    val top =
      (result
       .filter { case (id, vert) => vert.value >= threshold }
       .map { case (id, vert) =>
         "%s\t%s\n".format(id, vert.value) }
       .collect.mkString)
    println(top)
  }
}

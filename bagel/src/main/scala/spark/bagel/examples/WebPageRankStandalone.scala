package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

object WebPageRankStandalone {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WebPageRankStandalone <inputFile> <threshold> <numIterations> <numSplits> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.PRSerializer")
    System.setProperty("spark.kryo.registrator", classOf[WGKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val numSplits = args(3).toInt
    val host = args(4)
    val usePartitioner = args(5).toBoolean
    val sc = new SparkContext(host, "WebPageRankStandalone")

    val startTime = System.currentTimeMillis

    val vertices = sc.objectFile[(Long, PRVertex)](inputFile, numSplits).cache()

    val n = vertices.count()
    val a = 0.15
    val links = vertices.map { case (id, v) => (id, v.outEdges) }.cache()
    var ranks = vertices.map { case (id, v) => (id, v.value) }
    val partitioner =
      if (usePartitioner) new CustomPartitioner(numSplits)
      else new HashPartitioner(numSplits)

    // Do the computation
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (Seq(links), Seq(rank))) =>
          links.map(dest => (dest, rank / links.size))
      }
      val combine = (x: Double, y: Double) => x + y
      ranks = (contribs.combineByKey(x => x, combine, combine, numSplits, partitioner)
               .mapValues(sum => a/n + (1-a)*sum))
    }

    // Print the result
    System.err.println("Pages with PageRank >= " + threshold + ":")
    val top =
      (ranks
       .filter { case (id, rank) => rank >= threshold }
       .map { case (id, rank) =>
         "%s\t%s\n".format(id, rank) }
       .collect.mkString)
    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
            .format(numIterations, time, time / numIterations))
    System.exit(0)
  }
}

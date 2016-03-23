package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Project1 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MartinHallenProject1")

    val spark = new SparkContext(conf)

	if (args.length != 3) {
		println("USAGE: Project1 inputfolder outputfolder tasknr")
		exit(1)	
	}

	val tasknr = args(2)

	val customers = spark.textFile(args(0) + "/customer.tbl").map(x => x.split('|'))
	val orders = spark.textFile(args(0) + "/orders.tbl").map(x => x.split('|'))

	val regex = ".*special.*requests.*".r

	val orderCount = orders.filter(x => !regex.pattern.matcher(x(8)).matches)
							.map(order => (order(1), 1)).reduceByKey(_ + _)

	val simpleCustomers = customers.map(customer => (customer(0),customer(0)))
	val customerOrders = simpleCustomers.leftOuterJoin(orderCount)

	val orderCustomers = customerOrders.map(x => (x._2._2, x._2._1))

	val orderCountCustomers = orderCustomers.aggregateByKey(0)((a :Int, _) => a + 1,_+_)

	val result = orderCountCustomers.sortByKey()

	val cleanResult = result.map({
		case (Some(a:Int), b) => List(a, b).mkString("|")
		case (_, b) => List(0, b).mkString("|")
	})

	cleanResult.collect.foreach(println)
	
	var outpath = ""

	if (tasknr=="1") {
		outpath = "/out_1"
	} else {
		outpath = "/out_2"
	}

	cleanResult.saveAsTextFile(args(1) + outpath)			

    spark.stop()
  }
}


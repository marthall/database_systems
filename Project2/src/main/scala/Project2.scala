package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Project2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MartinHallenProject2")

    val spark = new SparkContext(conf)

	if (args.length != 3) {
		println("USAGE: Project1 inputfolder outputfolder tasknr")
		exit(1)	
	}

	val tasknr = args(2)

	val orders = spark.textFile(args(0) + "/orders.tbl").flatMap(x => {
		val fields = x.split('|')
		val customerKey = fields(1).toInt
		val orderDetails = fields(8)
		List((customerKey, orderDetails))
	})

	val regex = ".*special.*requests.*".r

	val orderCount = orders.filter(x => !regex.pattern.matcher(x._2).matches)
					.map(order => (order._1, 1)).reduceByKey(_ + _)

	if (tasknr=="1") {
		val customers = spark.textFile(args(0) + "/customer.tbl").flatMap(x => {
			val fields = x.split('|')
			val customerKey = fields(0).toInt
			List((customerKey, customerKey))
		})

		val customerOrders = customers.leftOuterJoin(orderCount)

		val orderCustomers = customerOrders.map(x => (x._2._2, x._2._1))

		val orderCountCustomers = orderCustomers.aggregateByKey(0)((a :Int, _) => a + 1,_+_)

		val result = orderCountCustomers.sortByKey()

		val cleanResult = result.map({
			case (Some(a:Int), b) => List(a, b).mkString("|")
			case (_, b) => List(0, b).mkString("|")
		})

		cleanResult.saveAsTextFile(args(1) + "/out_1")	
	
	} else {
		val orderCustomers = orderCount.map(x => (x._2, x._1))
		val orderCountCustomers = orderCustomers.aggregateByKey(0)((a :Int, _) => a + 1,_+_)
		val result = orderCountCustomers.sortByKey()

		val cleanResult = result.map(x =>
			x._1 + "|" + x._2
		)

		cleanResult.saveAsTextFile(args(1) + "/out_2")	
	}
	
	// cleanResult.collect.foreach(println)

    spark.stop()
  }
}


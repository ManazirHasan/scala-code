import org.apache.spark.sql.SparkSession

import java.util

object MainTest {
	def main(args: Array[String]): Unit = {
		println("Hello world!----")
		var ss = new SparkSessionTest().sessionObject();
		var x = ss.read.option("header", "true").csv("employee.csv");
		x.printSchema();
    x.show();
    var arrayCol = Seq("emp_name","emp_address");
    x = x.na.drop(arrayCol);
    x.show();
    //x = x.groupBy("emp_number").pivot("emp_name").count()
    //x.show();


		/*var myList1 = List(1, 2, 3, 4, 5);
		var myList = scala.collection.mutable.Set(1, 2, 3, 4, 5, 6, 1);
		println(myList)
		var a = A
		a.test();*/
	}



	object A {
		var id: Integer = 0;
		var name: String = "";
		var location: String = "";

		def test(): Unit = {
			println("------test method call-----");
		}
	}
}



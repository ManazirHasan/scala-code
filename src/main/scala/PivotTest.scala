import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructType}

object PivotTest {

	def main(args: Array[String]): Unit = {
		println("------------Test--------")
		var session = new SparkSessionTest().sessionObject();
		println(session)
		var rdd = session.read.option("header","true").csv("concentrix1.csv");
		rdd.show()
		rdd = rdd.groupBy("owner").pivot("pet").count()
		rdd.na.fill(0,Array("test"))
		rdd.createTempView("t")
		rdd = session.sql("select owner,nvl(`null`,'Test') as col_null,nvl(cat,'') as cat ,nvl(dog,'') as dog,nvl(horse,'') as horse from t ")
		rdd.show()



	}
}

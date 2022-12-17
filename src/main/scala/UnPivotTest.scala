import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object UnPivotTest {

	def main(args: Array[String]): Unit = {
		println("------------Test--------")
		var session = new SparkSessionTest().sessionObject();
		println(session)
		/*var rdd = session.read.option("header","true").csv("concentrix1.csv");
		rdd.show()
		rdd = rdd.groupBy("owner").pivot("pet").count()
		rdd.createTempView("t")
		rdd = session.sql("select owner,nvl(`null`,'') as col_null,nvl(cat,'') as cat ,nvl(dog,'') as dog,nvl(horse,'') as horse from t ")
		rdd.show()*/

		var rdd = session.read.option("header","true").csv("concentrix2.csv");
		rdd.show()
		rdd.createTempView("t");
		rdd = session.sql("select (count(*) - count(name)) as name,(count(*) - count(age)) as age,(count(*) - count(address)) as address from t")
		rdd.show()
		var x = rdd.schema.fieldNames
		var new_column="col_name,column_null";
		var seq= Seq(Row(x(0),rdd.select(x(0)).first().get(0)),Row(x(1),rdd.select(x(1)).first().get(0)),Row(x(2),rdd.select(x(2)).first().get(0)))
		var column_null = new StructType().add("col_name",StringType).add("column_null",LongType);
		val df = session.createDataFrame(session.sparkContext.parallelize(seq),column_null)
		df.show()

	}
}

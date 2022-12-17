import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class SparkSessionTest {

	def sessionObject(): SparkSession = {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		var ss = SparkSession.builder().master("local[2]").getOrCreate();
		return ss;
	}

}

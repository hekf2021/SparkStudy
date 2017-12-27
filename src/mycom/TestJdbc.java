package mycom;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestJdbc {

	public static void main(String[] args) {
		String warehouseLocation = "/user/hive/warehouse";
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Example")
		  //.config("spark.sql.warehouse.dir", warehouseLocation)
		  .enableHiveSupport()
		  .master("local")
		  //.master("slave1.huacloud.test")
		  .getOrCreate();
		
		/*//连Mysql
		Dataset<Row> jdbcDF = spark.read()
				  .format("jdbc")
				  .option("url", "jdbc:mysql://10.111.134.55:3306/oa?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull")
				  .option("driver", "com.mysql.jdbc.Driver")
				  .option("dbtable", "userinfo")
				  .option("user", "root")
				  .option("password", "123456")
				  .load();
		jdbcDF.show();*/
		
		//连hive
		Dataset<Row> jdbcDF = spark.read()
						  .format("jdbc")
						  .option("url", "jdbc:hive2://10.111.134.55:10000/oa")
						  .option("driver", "org.apache.hive.jdbc.HiveDriver")
						  .option("dbtable", "userinfo")
						  .option("user", "root")
						  .option("password", "")
						  .load();
				jdbcDF.show();

	}

}

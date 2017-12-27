package mycom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
//import org.elasticsearch.client.Client;



//import dao.EsBaseDao;
import model.Person;
import model.Record;
public class Test5 {

	public static void main(String[] args) {
		//System.setProperty("HADOOP_USER_NAME", "hdfs"); 
		String warehouseLocation = "/user/hive/warehouse";
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Example")
		  .config("spark.sql.warehouse.dir", warehouseLocation)
		  .enableHiveSupport()
		  .master("local")
		  //.master("slave1.huacloud.test")
		  .getOrCreate();
		
		spark.sql("select * from src").show();
		
		//EsBaseDao dao = new EsBaseDao();
		//Client client = dao.getClient("myindex");
		
		
		
		
	}

}

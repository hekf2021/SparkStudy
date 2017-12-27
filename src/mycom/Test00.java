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

import model.Person;
import model.Record;
public class Test00 {

	public static void main(String[] args) {
		String warehouseLocation = "/user/hive/warehouse";
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Example")
		  .config("spark.sql.warehouse.dir", warehouseLocation)
		  .enableHiveSupport()
		  .master("local")
		  //.master("slave1.huacloud.test")
		  .getOrCreate();
		
		spark.sql("use jingzong");
		spark.sql("select * from es_pramiry").show();
		
		
		
	}

}

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
public class Test3 {

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
		
		spark.sql("use oa");
		System.out.println("1111111111111");
		spark.sql("select * from userinfo").show();
		//System.setProperty("HADOOP_USER_NAME", "hadoop");  
		//System.setProperty("HADOOP_USER_NAME", "hdfs");  
		//1、创建表，以及load数据
		//spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
		//spark.sql("LOAD DATA LOCAL INPATH 'D:/tool/spark-2.0.0/examples/src/main/resources/kv1.txt' INTO TABLE src");

		//2、查询结果处理中间数据
		/*spark.sql("SELECT * FROM src").groupBy("value").count().show();
		spark.sql("SELECT * FROM hive.hivetable").show();
		Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");
		Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
		  @Override
		  public String call(Row row) throws Exception {
		    return "Key: " + row.get(0) + ", Value: " + row.get(1);
		  }
		}, Encoders.STRING());
		stringsDS.show();*/
		
		/*
		//3、表关联查询
		List<Record> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
		  Record record = new Record();
		  record.setKey(key);
		  record.setValue("val_" + key);
		  records.add(record);
		}
		Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");
		spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();*/
		
		
		
		//4、jdbc方式连接，没有测试通过
		/*Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "");
		Dataset<Row> jdbcDF2 = spark.read().jdbc("jdbc:hive2://10.111.134.54:10000/hive", "hivetable", connectionProperties);
		Dataset<Row> sqlDF = spark.sql("SELECT a.* FROM hivetable a");
		Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
			  @Override
			  public String call(Row row) throws Exception {
			    return "Key: " + row.get(0) + ", Value: " + row.get(1);
			  }
		}, Encoders.STRING());
		stringsDS.show();*/
		
		
		
		/*
		//5、处理数据
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
		  @Override
		  public Integer call(Integer value) throws Exception {
		    return value + 1;
		  }
		}, integerEncoder);
		transformedDS.collect(); // Returns [2, 3, 4]
		transformedDS.show();*/
		
		
		//6、将查询结果保存为结果表
		//spark.sql("SELECT * FROM src").groupBy("value").count().show();
		//spark.sql("SELECT * FROM src where value in ('val_80','val_118','val_409','val_47')").groupBy("value").count().write().saveAsTable("src3");
		
		//JavaRDD<Row> rdd = spark.sql("SELECT * FROM src").groupBy("value").count().javaRDD();
		
		
		/*// The schema is encoded in a string
		String schemaString = "value count";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> peopleDataFrame = spark.createDataFrame(rdd, schema);

		// Creates a temporary view using the DataFrame
		peopleDataFrame.createOrReplaceTempView("people");*/
		
		//spark.sql("insert into src1 select * from src2");
		
		//spark.sql("select * from hive_carflowitem2").show();
		
		//System.out.println("num="+num);
		/*;
		//7、删除表
		spark.sql("drop table src1");
		System.out.println("11111111111111");
		spark.sql("SELECT * FROM src1").show();*/
		
		/*spark.sql("use hive");
		spark.sql("SELECT name as abc FROM hivetable").groupBy("abc").count().show();*/
		
		/*Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Row> dataSet = spark.sql("select s.key,s.value,t.count from src s left join src1 t on s.value=t.value");
		int i=0;
		dataSet.map(new MapFunction<Row, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Row row) throws Exception {
				  System.out.println("Key: " + row.get(0) + ", Value: " + row.get(1)+" ,count: "+row.getByte(2));
				  return 1;
			}
			
		}, integerEncoder);*/
		
		/*List<Row> list = spark.sql("select s.key,s.value,t.count from src s left join src1 t on s.value=t.value").collectAsList();
		System.out.println("size= "+list.size());*/
		
		/*Dataset<Row> sqlDF = spark.sql("select s.key,s.value,t.count from src s left join src1 t on s.value=t.value");
		long count=sqlDF.map(new MapFunction<Row, String>() {
			  private int index =0;
			  @Override
			  public String call(Row row) throws Exception {
				  System.out.println(index++);
				  System.out.println("Key: " + row.get(0) + ", Value: " + row.get(1)+" ,count: "+row.get(2));
			    return null;
			  }
		}, Encoders.STRING()).count();*/
		//System.out.println("count="+stringsDS.count());
		
		
	}

}

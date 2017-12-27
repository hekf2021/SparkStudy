package mycom;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import model.Person;

import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class Test1 {
	public static void main(String[] args){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("SparkSQL")
				  .master("local")
				  .getOrCreate();
		
		/*
		//1、创建临时表
		Dataset<Row> df = spark.emptyDataFrame();
		df.createOrReplaceTempView("people");
		Dataset<Row> sqlDF = spark.sql("select * from people");
		sqlDF.show();*/
		
		/*
		//2、创建数据集
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);

		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
		  Collections.singletonList(person),
		  personEncoder
		);
		javaBeanDS.show();*/
		
	
		/*
		//3、读取json文件
		String path = "examples/src/main/resources/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
		*/
		
	}
}

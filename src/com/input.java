/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.List;
import java.util.Random;

/**
 * Computes an approximation to pi Usage: JavaSparkPi [slices]
 */
public final class input {

	public static void main(String[] args) throws Exception {
		
		Random r = new Random();  
		int num = r.nextInt(100); 
		//String path ="d:/a/foo.txt";
		String path="./foo.txt";
		SparkSession spark = SparkSession.builder()
				.appName("JavaSparkPi"+num)
				//.master("local")
				.getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> line = jsc.textFile(path);
		List<String> list = line.collect();
		for(String str :args){
			System.out.println("带参进的="+str);
		}
		for(String str : list){
			System.out.println("文件里的信息="+str);
		}
		for(int i=0;i<5;i++){
			System.out.println("当前执行到="+i);
			Thread.currentThread().sleep(10000);
		}
		spark.stop();
		
	}

	
}

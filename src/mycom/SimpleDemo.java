package mycom;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
 
public class SimpleDemo {
    public static void main(String[] args) {
    	String[] jars = new String[]{"/data/houxm/spark/spark.jar"};
        SparkConf conf = new SparkConf().setAppName("simpledemo").setMaster("yarn-client").set("executor-memory", "2g").setJars(jars).set("driver-class-path", "/data/spark/lib/mysql-connector-java-5.1.21.jar");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveCtx = new HiveContext(sc);
        testHive(hiveCtx);
        sc.stop();
        sc.close();
    }
 
    //测试spark sql查询hive上面的表
    public static void testHive(HiveContext hiveCtx) {
        hiveCtx.sql("create table temp_spark_java as select mobile,num from default.mobile_id_num02 limit 10");
    }
}
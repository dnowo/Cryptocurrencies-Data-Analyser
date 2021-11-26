package pl.rpd.projekt.spark;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static pl.rpd.projekt.Constants.*;

@Component
public class ApacheSpark {

    private SparkSession sparkSession;

    @PostConstruct
    public void setup() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("");
        sparkConf.set("spark.cassandra.connection.host", CASSANDRA_HOST);
        sparkConf.set("spark.cassandra.auth.username", CASSANDRA_USERNAME);
        sparkConf.set("spark.cassandra.auth.password", CASSANDRA_PASSWORD);
        sparkConf.set("spark.cassandra.output.consistency.level", "ONE");

        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    public void proceedData() {
        Dataset<Row> empDeptDataSet = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "crypto")
                .option("table", "cryptocurrencies")
                .load();

        empDeptDataSet.select("volume").show(100);

// Example jakies cos nie wiem jeszcze po co

//        JavaRDD<String> idWithNameConcatenated = lines.flatMap(line -> {
//            String[] data = COMMA.split(line);
//            List<String> results = new ArrayList<>();
//
//            String id = data[1];
//            String name = data[2];
//            results.add(id + " " + name);
//            return results;
//        });
//
//        JavaRDD<Integer> amount = lines.flatMap(line -> {
//            String[] data = COMMA.split(line);
//            List<Integer> results = new ArrayList<>();
//
//            int returnAmount = Integer.parseInt(data[2]);
//            results.add(returnAmount);
//            return results;
//        });
//        JavaPairRDD<String, Integer> idNameWithAmount = idWithNameConcatenated.zip(amount);
//        idWithNameConcatenated.collect();
//        JavaPairRDD<String, Integer> counts = idNameWithAmount.reduceByKey(Integer::sum);
//        JavaPairRDD<String, Integer> sorted = counts.sortByKey();
//
//        sorted.coalesce(1).saveAsHadoopFile(outputPath, Text.class, IntWritable.class, SequenceFileOutputFormat.class);
//        sc.stop();
    }
}

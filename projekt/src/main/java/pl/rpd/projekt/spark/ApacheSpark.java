package pl.rpd.projekt.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

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
        Dataset<Row> crypto = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "crypto")
                .option("table", "cryptocurrencies")
                .load();
        Dataset<Row> dates = crypto.select("date");
        Encoder<Timestamp> timestampEncoder = Encoders.TIMESTAMP();
        Dataset<Timestamp> timestampDataset = dates.map((MapFunction<Row, Timestamp>) row -> {
            var r = row.getTimestamp(0).toInstant();
            if (r.isAfter(Instant.now().minus(365, ChronoUnit.DAYS))) {
                return Timestamp.from(r);
            }
            return null;
        }, timestampEncoder).filter(Objects::nonNull);
        timestampDataset.show(30, false);

        JavaRDD<Timestamp> timestamps = timestampDataset.javaRDD();


//        lines.flatMap(line -> {
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

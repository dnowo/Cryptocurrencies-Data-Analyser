package pl.rpd.projekt.spark;

import jnr.ffi.annotations.In;
import lombok.val;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.text.TextFileFormat;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
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
        Dataset<Row> cryptoObj = crypto.select("date", "open", "close").where("symbol = 'DOGE'");
        Encoder<MiniCryptoDto> encoder = Encoders.bean(MiniCryptoDto.class);
        Dataset<MiniCryptoDto> cryptoDataset = cryptoObj.map((MapFunction<Row, MiniCryptoDto>) row -> {
            val r = row.getTimestamp(0).toInstant();
            if (r.isAfter(Instant.now().minus(365, ChronoUnit.DAYS))) {
                val mcdto = new MiniCryptoDto();
                mcdto.setOpen(row.getDecimal(1));
                mcdto.setClose(row.getDecimal(2));
                mcdto.setDate(Timestamp.from(r));
                mcdto.setDifference(mcdto.getClose().subtract(mcdto.getOpen()).abs());
                return mcdto;
            }
            return null;
        }, encoder).filter(obj -> obj.getDate() != null);
        cryptoDataset.show(30, false);

        Encoder<Integer> encoderTimestamp = Encoders.INT();
        Dataset<Integer> datasetTimestamp = cryptoDataset.select("date")
                .map((MapFunction<Row, Integer>) row -> Integer.parseInt("" + (row.getTimestamp(0).getTime() / 1000)), encoderTimestamp);

        Encoder<Double> encoderDiff = Encoders.DOUBLE();
        Dataset<Double> datasetDiff = cryptoDataset.select("difference")
                .map((MapFunction<Row, Double>) row -> Double.parseDouble("" + row.getDecimal(0)), encoderDiff);

        JavaRDD<Integer> dates = datasetTimestamp.javaRDD();
        JavaRDD<Double> differences = datasetDiff.javaRDD();
        JavaPairRDD<Integer, Double> datesDifferencesPair = dates.zip(differences).sortByKey();
        JavaPairRDD<IntWritable, DoubleWritable> datesDifferencesPairWritable = datesDifferencesPair.mapToPair(new ConvertToWritableTypes());
        datesDifferencesPairWritable.coalesce(1).saveAsHadoopFile("hdfs://localhost:9000/sparkInput", IntWritable.class, DoubleWritable.class, TextOutputFormat.class);
        sparkSession.stop();
    }

    public static class ConvertToWritableTypes implements PairFunction<Tuple2<Integer, Double>, IntWritable, DoubleWritable> {
        public Tuple2<IntWritable, DoubleWritable> call(Tuple2<Integer, Double> record) {
            return new Tuple2(new IntWritable(record._1), new DoubleWritable(record._2));
        }
    }
}

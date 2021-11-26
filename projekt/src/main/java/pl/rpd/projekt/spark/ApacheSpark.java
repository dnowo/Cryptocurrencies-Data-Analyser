package pl.rpd.projekt.spark;

import org.apache.spark.SparkConf;
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

        empDeptDataSet.show(1);
        System.out.println("xD");
    }
}

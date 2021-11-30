package pl.rpd.projekt.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ApacheHadoop {
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",   // driver class
                "jdbc:mysql://localhost:3306/baza?serverTimezone=UTC", // db url
                "root",    // username
                "password"); //password

        Job job = new Job(conf);
        job.setJarByClass(ApacheHadoop.class);
        job.setMapperClass(DBMapper.class);
        job.setReducerClass(DBReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/sparkInput"));

        DBOutputFormat.setOutput(
                job,
                "tabela", "name", "count"
        );

        job.waitForCompletion(true);
    }
}
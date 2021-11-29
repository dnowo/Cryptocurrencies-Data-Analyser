package pl.rpd.projekt.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ApacheHadoop {
    public void setup() throws Exception{
        JobConf conf = new JobConf(ApacheHadoop.class);
        conf.setJobName("hadoopDemo");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Mapper.class);
        conf.setCombinerClass(Reducer.class);
        conf.setReducerClass(Reducer.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path("hdfs://localhost:19000/sparkInput"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://localhost:19000/hadoopOutput"));
        JobClient.runJob(conf);
    }
}
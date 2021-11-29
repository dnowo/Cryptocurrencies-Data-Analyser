package pl.rpd.projekt.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;



public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<IntWritable, DoubleWritable, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text FIRST = new Text("<0.00001");
    private final static Text second = new Text("0.00001 - 0.0001");
    private final static Text third = new Text("0.0001 - 0.001");
    private final static Text forth = new Text("0.001 - 0.01");
    private final static Text fifth = new Text(">=0.01");
    @Override
    public void map(IntWritable key, DoubleWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        System.out.println("MAP");
        Double diff = Double.parseDouble(value.toString());
        if (diff < 0.00001) {

            output.collect(FIRST, ONE);
        } else if (diff < 0.0001) {

            output.collect(second, ONE);
        } else if (diff < 0.001) {

            output.collect(third, ONE);
        } else if (diff < 0.01) {

            output.collect(forth, ONE);
        } else {

            output.collect(fifth, ONE);
        }
    }
}

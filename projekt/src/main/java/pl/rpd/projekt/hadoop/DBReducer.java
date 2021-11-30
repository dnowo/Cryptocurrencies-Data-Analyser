package pl.rpd.projekt.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DBReducer extends Reducer<Text, IntWritable, DBOutputWritable, NullWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        DBOutputWritable productRecord = new DBOutputWritable(key.toString(), sum);
        context.write(productRecord, NullWritable.get());
    }
}

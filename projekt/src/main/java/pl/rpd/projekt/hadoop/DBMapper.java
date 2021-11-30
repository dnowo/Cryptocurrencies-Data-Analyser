package pl.rpd.projekt.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class DBMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text FIRST = new Text("<0.00001");
    private final static Text second = new Text("0.00001 - 0.0001");
    private final static Text third = new Text("0.0001 - 0.001");
    private final static Text forth = new Text("0.001 - 0.01");
    private final static Text fifth = new Text(">=0.01");

    protected void map(LongWritable key, Text value, Context ctx)
    {
        try
        {
            double diff = Double.parseDouble(value.toString().split("\t")[1]);
            if (diff < 0.00001) {
                ctx.write(FIRST, ONE);
            } else if (diff < 0.0001) {
                ctx.write(second, ONE);
            } else if (diff < 0.001) {
                ctx.write(third, ONE);
            } else if (diff < 0.01) {
                ctx.write(forth, ONE);
            } else {
                ctx.write(fifth, ONE);
            }

        } catch(IOException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
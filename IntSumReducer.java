package tn.isima.twitter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    //private IntWritable result = new IntWritable();

    public void reduce(final Text key,
                       final Iterable<DoubleWritable> values,
                       final Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        Integer count=0;
        for (DoubleWritable value : values) {
            //System.out.println("value: "+value.get());
            sum += value.get();
            count++;
        }
        //System.out.println("--> Sum = "+sum);
        //result.set(sum);
        context.write(key, new DoubleWritable(sum));

    }
}

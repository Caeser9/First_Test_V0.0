package tn.isima.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
        extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0 ;
        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }


        context.write(key, new DoubleWritable(sum));
    }
}

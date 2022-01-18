package tn.isima.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
        String line = value.toString();
        String[] data = line.split(",");

        try {
            String Name = data[0];
            Double Shares = Double.parseDouble(data[9]);

            context.write(new Text(Name), new DoubleWritable(Shares));
        } catch (Exception e){}
    }

}

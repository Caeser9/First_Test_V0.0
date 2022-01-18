package tn.isima.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //private String Influencer;

    public void map(Object key, Text value, Mapper.Context context
    )
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(",");

        Double Shares = 0.0;
        String Influencer = "";
        Double Likes = 0.0;
        try {
            Influencer = data[0];
            Shares = Double.parseDouble(data[9]);
            Likes = Double.parseDouble(data[8]);
            context.write (new Text(Influencer), new DoubleWritable(Shares));

        } catch (Exception e) {

        }
        System.out.println(" Influencer= " + Influencer + " , Shares " + Shares + ", Likes" + Likes);
    }
    }

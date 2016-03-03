package fr.ecp.sio;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by abdelhafiz on 26/02/16.
 */
public class MonMapper extends Mapper<LongWritable,Text,LongWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        context.write(key,value);
    }



}

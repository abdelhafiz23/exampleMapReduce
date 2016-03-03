package fr.ecp.sio;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by abdelhafiz on 26/02/16.
 */
public class MonMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {

    //private final static Logger LOGGER =

    private final static Splitter SPLITTER =Splitter.on(";").omitEmptyStrings().trimResults();
    private  final Text key = new Text();
    private final FloatWritable value = new FloatWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        final List<String> tokens = Lists.newArrayList(SPLITTER.split(line.toString()));

        try {
            final Float temperature = Float.valueOf(tokens.get(9));
            key.set(tokens.get(0));
            value.set(temperature);
            context.write(key,value);
        }catch (RuntimeException e){

            context.getCounter("PARSING","Temperature Error").increment(1l);
        }



    }



}

package fr.ecp.sio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by abdelhafiz on 03/03/16.
 */
public class MonReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        long numberOfOccurences = 0l;
        float sum = 0f;

        for (FloatWritable value:values){
            sum+=value.get();
            numberOfOccurences++;
        }
        Float average = sum/numberOfOccurences;
        context.write(key,new FloatWritable(average));
    }
}

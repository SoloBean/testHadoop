package apress.testhadoop.ad2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReadReducer extends Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
        //StringBuilder res = new StringBuilder(key.toString()).append("\t");
        for (Text value : values){
            String res = key.toString() + "\t" + value.toString();
            context.write(NullWritable.get(), new Text(res.toString()));
        }
    }
}

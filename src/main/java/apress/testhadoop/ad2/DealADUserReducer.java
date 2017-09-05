package apress.testhadoop.ad2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DealADUserReducer extends Reducer<UserAndIdWritable, IntWritable, NullWritable, Text> {
    public void reduce(UserAndIdWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        Text user = key.getUser();
        Text ID = key.getID();
        StringBuilder str = new StringBuilder(user.toString()).append("\t");
        str.append(ID.toString()).append("\t");
        str.append(sum);
        context.write(NullWritable.get(), new Text(str.toString()));
    }
}

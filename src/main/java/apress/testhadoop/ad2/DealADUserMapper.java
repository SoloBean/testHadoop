package apress.testhadoop.ad2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DealADUserMapper extends Mapper<LongWritable, Text, UserAndIdWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] str = value.toString().split(",");
        UserAndIdWritable user = new UserAndIdWritable(new Text(str[0]), new Text(str[1]));
        IntWritable count = new IntWritable(Integer.valueOf(str[2]));
        context.write(user, count);
    }
}

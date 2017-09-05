package apress.testhadoop.ad2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
        String[] str = value.toString().split("\t");
        StringBuilder tmp = new StringBuilder();
        for (int i = 1; i < str.length; i++){
            tmp.append(str[i]).append("\t");
        }
        context.write(new Text(str[0]), new Text(tmp.toString()));
    }
}

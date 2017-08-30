package apress.testhadoop.ad2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DealADUser extends Configured implements Tool{
    public static class MyMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String[] tmp = value.toString().split(",");
            Text usr = new Text(tmp[0]);
            MapWritable map = new MapWritable();
            map.put(new Text(tmp[1]), new IntWritable(Integer.valueOf(tmp[2])));
            context.write(usr, map);
        }
    }

    public static class MyReducer extends Reducer<Text, MapWritable, NullWritable, Text>{
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException{
            MapWritable tmp = new MapWritable();
            for (MapWritable value : values){
                for (Writable id : value.keySet()){
                    Text ID = (Text)id;
                    IntWritable count = (IntWritable) value.get(ID);
                    if (!tmp.containsKey(ID)){
                        tmp.put(ID, count);
                    }
                    else {
                        IntWritable sum = new IntWritable(count.get() + ((IntWritable) value.get(ID)).get());
                        tmp.put(ID, sum);
                    }
                }
            }
            for (Writable id : tmp.keySet()){
                StringBuilder str = new StringBuilder();
                str.append(key.toString()+"\t");
                Text ID = (Text)id;
                str.append(ID.toString()+"\t");
                IntWritable num = (IntWritable) tmp.get(ID);
                str.append(num.get());
                context.write(NullWritable.get(), new Text(str.toString()));
            }
        }
    }

    public int run(String[] allArgs) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJarByClass(DealADUser.class);
        FileInputFormat.addInputPath(job, new Path(allArgs[0]));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ToolRunner.run(new DealADUser(), args);
    }
}

package apress.testhadoop.ad;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DealAD extends Configured implements Tool{
    public static class MyMapper extends Mapper<LongWritable, Text, UserAndIdWritable, IntWritable>{
        //@Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String str = value.toString();
            String[] tmp = str.split(",");
            UserAndIdWritable user = new UserAndIdWritable(new Text(tmp[0]), new Text(tmp[1]));
            IntWritable count = new IntWritable(Integer.valueOf(tmp[2]));
            context.write(user, count);
        }
    }

    public static class MyReducer extends Reducer<UserAndIdWritable, IntWritable, NullWritable, Text>{
        //@Override
        public void reduce(UserAndIdWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable value : values){
                sum += value.get();
            }
            StringBuilder res = new StringBuilder();
            res.append(key.User.toString() + ",");
            res.append(key.ID.toString() + ",");
            res.append(sum);
            context.write(NullWritable.get(), new Text(res.toString()));
        }
    }

    public int run(String[] allArgs) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJarByClass(UserAndIdWritable.class);
        FileInputFormat.addInputPath(job, new Path(allArgs[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(UserAndIdWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //You can set any number of reducers you want. 12 is just a number I picked.
        job.setNumReduceTasks(1);


        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ToolRunner.run(new DealAD(), args);
    }
}

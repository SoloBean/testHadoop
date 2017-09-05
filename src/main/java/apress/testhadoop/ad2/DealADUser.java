package apress.testhadoop.ad2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

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

    String[] dataInpuPath = {"hdfs://master:9000/user/dbcluster/ADintput"};
    String[] dataOutPath = {"hdfs://master:9000/user/dbcluster/ADoutput"};

    public void setup(Configuration configuration) throws Exception {
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"), configuration);

        Path input = new Path(dataInpuPath[0]);
        Path output = new Path(dataOutPath[0]);

        if (hdfs.exists(output)){
            hdfs.delete(output, true);
        }
    }

    public int run(String[] allArgs) throws Exception{
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        Configuration jobConf = job.getConfiguration();
        setup(conf);

        job.setJobName("DealADApplication");
        job.setJarByClass(DealADUser.class);
        String[] inputPath = dataInpuPath[0].split("/");
        //FileInputFormat.addInputPath(job, new Path("ADinput"));
        job.setInputFormatClass(TextInputFormat.class);
        jobConf.set("mapred.input.dir", "ADinput");
        //jobConf.setBoolean("mapred.compress.map.output", true);
        job.setOutputFormatClass(TextOutputFormat.class);
        jobConf.set("mapred.output.dir", "ADoutput");

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        //String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        //FileOutputFormat.setOutputPath(job, new Path("ADoutput"));
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ToolRunner.run(new DealADUser(), args);
    }
}

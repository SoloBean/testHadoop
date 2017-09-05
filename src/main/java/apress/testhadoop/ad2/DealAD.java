package apress.testhadoop.ad2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class DealAD  extends Configured implements Tool{

    public void setup(String outputPath) throws Exception{
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"), getConf());
        StringBuilder tmp = new StringBuilder("hdfs://master:9000/user/dbcluster/");
        tmp.append(outputPath);
        Path output = new Path(tmp.toString());
        if (hdfs.exists(output)){
            hdfs.delete(output, true);
        }
    }

    public int run(String[] allArgs) throws Exception{
        Configuration conf = getConf();

        //String[] str = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        String output = "ADoutput";
        String input =  "ADinput";
        setup(output);
        HadoopUtils utils = new HadoopUtils();
        Job job1 = utils.perpareJob("DealAD", input, output, TextInputFormat.class, DealADUserMapper.class,
                UserAndIdWritable.class, IntWritable.class, DealADUserReducer.class,
                NullWritable.class, Text.class, TextOutputFormat.class, conf);

        Configuration jobConf = job1.getConfiguration();
        String ss = jobConf.get("mapred.output.dir");
        System.out.println(ss+"---------------------------------------------------------------");
        Job job2 = utils.perpareJob("readJob", input, output, TextInputFormat.class, ReadMapper.class,
                Text.class, Text.class, ReadReducer.class, NullWritable.class, Text.class,
                TextOutputFormat.class, conf);

        if (job1.waitForCompletion(true)){
            setup(output);
            job2.waitForCompletion(true);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        for (int i = 0; i < 2; i++){
            Configuration conf = new Configuration();
            ToolRunner.run(new DealAD(), args);
        }
    }
}

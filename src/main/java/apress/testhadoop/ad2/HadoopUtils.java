package apress.testhadoop.ad2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopUtils extends Configured {
    public HadoopUtils(){}

    public Job perpareJob(String jobName,
                       String inputPath,
                       String outputPath,
                       Class<? extends InputFormat> inputFormat,
                       Class<? extends Mapper> mapper,
                       Class<? extends Writable> mapperKey,
                       Class<? extends Writable> mapperValue,
                       Class<? extends Reducer> reducer,
                       Class<? extends Writable> reducerKey,
                       Class<? extends Writable> reducerValue,
                       Class<? extends OutputFormat> outputFormat,
                       Configuration conf) throws Exception{
        Job job = Job.getInstance(conf);
        job.setJobName(jobName);
        job.setJarByClass(mapper);

        job.setInputFormatClass(inputFormat);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);
        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);
        job.setOutputFormatClass(outputFormat);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}

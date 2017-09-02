package apress.testhadoop.myReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MyInputFormat extends FileInputFormat<LongWritable, Text> {


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //返回自定义的RecordReader
        return new MyRecordReader();
    }

    /**
     * 为了使得切分数据的时候行号不发生错乱
     * 这里设置为不进行切分
     */
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

}

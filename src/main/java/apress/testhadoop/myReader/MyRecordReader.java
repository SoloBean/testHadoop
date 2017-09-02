package apress.testhadoop.myReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class MyRecordReader extends RecordReader<LongWritable, Text>{
    //起始位置(相对整个分片而言)
    private long start;
    //结束位置(相对整个分片而言)
    private long end;
    //当前位置
    private long pos;
    //文件输入流
    private FSDataInputStream fin = null;
    //key、value
    private LongWritable key = null;
    private Text value = null;
    //定义行阅读器(hadoop.util包下的类)
    private LineReader reader = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        //获取分片
        FileSplit fileSplit = (FileSplit) split;
        //获取起始位置
        start = fileSplit.getStart();
        //获取结束位置
        end = start + fileSplit.getLength();
        //创建配置
        Configuration conf = context.getConfiguration();
        //获取文件路径
        Path path = fileSplit.getPath();
        //根据路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(conf);
        //打开文件输入流
        fin = fileSystem.open(path);
        //找到开始位置开始读取
        fin.seek(start);
        //创建阅读器
        reader = new LineReader(fin);
        //将当期位置置为1
        pos = 1;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null){
            key = new LongWritable();
        }
        //设置行号
        key.set(pos);
        if (value == null){
            value = new Text();
        }
        if (reader.readLine(value) == 0){
            return false;
        }
        pos ++;

        return true;

    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value ;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return 0;
    }

    @Override
    public void close() throws IOException {
        fin.close();

    }
}

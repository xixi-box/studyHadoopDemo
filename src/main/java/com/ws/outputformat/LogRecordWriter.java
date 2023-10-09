package com.ws.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream wsOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //用文件系统对象创建两个输出流对应不同的目录
            wsOut = fs.create(new Path("Z:\\HadoopData\\ws.log"));
            otherOut = fs.create(new Path("Z:\\HadoopData\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        //根据一行的log数据是否包含ws,判断两条输出流输出的内容
        if (log.contains("ws")) {
            wsOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }
    }
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关流
        IOUtils.closeStream(wsOut);
        IOUtils.closeStream(otherOut);
    }
}
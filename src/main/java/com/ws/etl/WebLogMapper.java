package com.ws.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixibox
 * @version 1.0
 * @date 2023/10/16 10:19
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //
        boolean result = parseLog(line, context);
        if (!result)
            return;
        //写出
        context.write(value, NullWritable.get());

    }

    private static boolean parseLog(String line, Context context) throws IOException {

        //切割字符串
        String[] split = line.split(" ");
        if (split.length > 11)
            return true;
        else
            return false;
    }
}

package com.ws.partitionandcomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    /**
     * @param flowBean
     * @param text
     * @param i
     * @return
     */
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        //text是手机号
        String phone = text.toString();
        String substring = phone.substring(0, 3);
        int partition;
        if ("136".equals(substring)) partition = 0;
        else if ("137".equals(substring)) partition = 1;
        else if ("138".equals(substring)) partition = 2;
        else if ("139".equals(substring)) partition = 3;
        else partition = 4;
        return partition;
    }
}

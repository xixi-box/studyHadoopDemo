package com.ws.partitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    public FlowBean() {
    }

    /**
     * @param dataOutput
     * @throws IOException
     * @description 序列化
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * @param dataInput
     * @throws IOException
     * @description反序列化
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }
}

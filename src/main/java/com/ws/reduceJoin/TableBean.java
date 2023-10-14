package com.ws.reduceJoin;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xixibox
 * @version 1.0
 * @date 2023/10/12 10:52
 */
@Data
public class TableBean implements Writable {

    private String id;
    private String pid;
    private int amount;
    private String pname;
    //标记是什么表
    private String flag;

    public TableBean() {
    }

    /**
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);


    }

    /**
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {


        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }
}

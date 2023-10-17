package com.ws.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 *
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        URI uri = new URI("hdfs://hadoop102:8020");

        String user = "root";
        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //关闭资源
        fs.close();
    }


    @Test
    public void testMkdirs() throws IOException {
        // 2 创建目录
        fs.mkdirs(new Path("/inputtopN"));

    }

    @Test
    public void testCopyFromLocalFile() throws IOException {
        //参数一 删除原数据 参数二 是否允许覆盖，参数三 原数据路径 参数4 目的地路径
        fs.copyFromLocalFile(false, true, new Path("Z:\\HadoopData\\inputTopN\\data.txt"), new Path("hdfs://hadoop102/inputtopN"));
    }


    @Test
    public void testRm() throws IOException {
        //参数1：要删除的路径 参数2：是否递归删除
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"), false);
        //删除空目录
        fs.delete(new Path("/xiyou"), false);
        //删除非空目录
        fs.delete(new Path("/jinguo"), true);
    }

    //移动文件
    @Test
    public void testMv() throws IOException {
        //参数1 原文件路径 参数二目标文件路径
        //名称修改
        fs.rename(new Path("/input/word.txt"), new Path("/input/ss.txt"));
        //移动和修改
        fs.rename(new Path("/input/a.txt"), new Path("/output/b.txt"));
        //目录更名
        fs.rename(new Path("/input"), new Path("/output"));
    }

    //获取文件详细信息
    @Test
    public void testInfo() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath());
            System.out.println(next.getOwner());
            System.out.println(next.getBlockLocations());
        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录" + fileStatus.getPath().getName());
            }
        }
    }
}
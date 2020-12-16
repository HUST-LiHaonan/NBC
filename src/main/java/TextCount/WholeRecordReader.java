/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: WholeRecordReader
 * Author:   mac
 * Date:     2020/10/2 8:09 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package TextCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 〈统计文档个数〉<br>
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class WholeRecordReader extends RecordReader<NullWritable,BytesWritable>{
    BytesWritable value = new BytesWritable();
    boolean isProcess = false;
    FileSplit split;
    Configuration configuration;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    /**
     * 将一个spilt作为一个k-v键值对
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //该spilt没有被处理
        if (!isProcess) {
            //设置缓冲区
            byte[] buffer = new byte[(int) split.getLength()];
            Path path = split.getPath();
            FileSystem fileSystem = null;
            FSDataInputStream fsDataInputStream = null;
            try {
                //获取文件系统
                fileSystem = path.getFileSystem(configuration);
                //根据spilt路径打开输入流
                fsDataInputStream = fileSystem.open(path);
                //读取文件内容
                IOUtils.readFully(fsDataInputStream, buffer, 0, buffer.length);
                //将文件内容赋值给value
                value.set(buffer, 0, buffer.length);
            } catch (Exception e) {
                //输出错误
                System.err.println(e);
            }finally {
                //关闭相应的流
                IOUtils.closeStream(fsDataInputStream);
                IOUtils.closeStream(fileSystem);
            }
            isProcess = true;
            return true;
        }
        return false;
    }

    /**
     * 获取当前键
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取当前值
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        // 获取当前值
        return value;
    }

    /**
     * 获取当前spile是否被处理
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        // 获取当前进度
        return isProcess ? 1:0;
    }

    @Override
    public void close() throws IOException {
    }
}

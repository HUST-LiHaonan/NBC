/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: ClassifyRecordReader
 * Author:   mac
 * Date:     2020/10/4 8:26 上午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Classify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/4
 * @since 1.0.0
 */
public class DelegateRecordReader extends RecordReader<NullWritable, Text> {

    FileSplit split;
    Configuration configuration;
    Text value = new Text();
    boolean isProcess = false;
    //这里利用代理模式，对LineRecordReader进行包装，使得该RecordReader能够将一个文件的所有内容读到一行中
    LineRecordReader lineRecordReader =new LineRecordReader();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 读取一整个文件的内容
        if (!isProcess) {
            String FileContext = "";
            while(lineRecordReader.nextKeyValue()){
                //通过LineRecordReader读取文件的每一行内容,并用'\t'分割
                String line = lineRecordReader.getCurrentValue() + "\t";
                //将内容拼接
                FileContext += line;
            }
            value.set(FileContext);
            isProcess = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // 获取当前进度
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
    }
}

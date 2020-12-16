/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: TestTextCountMapper
 * Author:   mac
 * Date:     2020/10/14 6:57 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Evalution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/14
 * @since 1.0.0
 */
public class TestTextCountMapper extends Mapper<NullWritable, BytesWritable,Text, IntWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        //获取当前分片
        FileSplit split = (FileSplit) context.getInputSplit();
        //获取分片的路径
        Path path = split.getPath();
        //获取文本的类型
        String classify = path.getParent().getName();
        k.set(classify);
        context.write(k,v);
    }
}

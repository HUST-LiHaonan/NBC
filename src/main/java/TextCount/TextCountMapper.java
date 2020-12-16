/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: TextCountMapper
 * Author:   mac
 * Date:     2020/10/2 8:15 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package TextCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 〈统计文档总数的Mapper〉<br>
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class TextCountMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {
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

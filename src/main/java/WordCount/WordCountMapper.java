/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: WordCountMapper
 * Author:   mac
 * Date:     2020/10/2 10:43 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 〈统计单词条件概率的Mapper〉<br>
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text,TextPair, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        TextPair k = new TextPair();
        //默认使用TextInputFormat，每次只读取一行即一个单词
        IntWritable v = new IntWritable(1);
        //获取当前分片
        FileSplit split = (FileSplit) context.getInputSplit();
        //获取当前文本所属的分类
        Text classify = new Text(split.getPath().getParent().getName());
        //将文本类型和单词组装成一个TextPair
        k.set(classify,value);
        context.write(k,v);
    }
}

/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: TestTextCountReducer
 * Author:   mac
 * Date:     2020/10/14 7:03 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Evalution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/14
 * @since 1.0.0
 */
public class TestTextCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text k = key;
        int count = 0;
        //统计当前类型文本的数量
        for (IntWritable value : values) {
            count += value.get();
        }
        IntWritable v = new IntWritable(count);
        context.write(k,v);
    }
}

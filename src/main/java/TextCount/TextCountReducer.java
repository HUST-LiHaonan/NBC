/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: TextCountReducer
 * Author:   mac
 * Date:     2020/10/2 8:19 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package TextCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 〈统计文档总数的Reducer〉<br>
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class TextCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
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

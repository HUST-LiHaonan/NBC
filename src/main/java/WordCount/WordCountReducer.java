/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: WordCountReducer
 * Author:   mac
 * Date:     2020/10/2 10:54 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 〈统计单词条件概率的Reducer〉<br>
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class WordCountReducer extends Reducer<TextPair, IntWritable,TextPair,IntWritable> {
    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        TextPair k = key;
        int count = 0;
        //统计当前类型的文本中该单词的数量
        for (IntWritable value : values) {
            count += value.get();
        }
        IntWritable v = new IntWritable(count);
        context.write(k,v);
    }
}

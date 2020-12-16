/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: ClassifyReducer
 * Author:   mac
 * Date:     2020/10/4 8:33 上午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Classify;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/4
 * @since 1.0.0
 */
public class ClassifyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Text k = key;
        Text v = new Text();
        String classification = "";
        //初始值设为负无穷
        double maxProb = Double.NEGATIVE_INFINITY;
        for(Text text:values) {
            String[] data = text.toString().split("#");
            //选择概率大的分类作为预测结果
            if(Double.parseDouble(data[1]) > maxProb){
                maxProb = Double.parseDouble(data[1]);
                classification = data[0];
            }
        }
        v.set(classification);
        context.write(k, v);
    }
}

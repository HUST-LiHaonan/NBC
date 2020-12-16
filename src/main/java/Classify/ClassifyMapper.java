/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: ClassifyMapper
 * Author:   mac
 * Date:     2020/10/4 8:30 上午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Classify;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/4
 * @since 1.0.0
 */
public class ClassifyMapper extends Mapper<NullWritable, Text, Text, Text> {
    @Override
    protected void setup(Mapper<NullWritable, Text, Text, Text>.Context context) {
        //加载相关资源，同时保证该类只会被加载一次
        try {
            Class.forName("Classify.BayesianClassification");
        }catch (ClassNotFoundException e){
            System.err.println(e);
        }
    }

    @Override
    protected void map(NullWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Text k = new Text();
        Text v = new Text();
        // 获取文件所属类别
        FileSplit split = (FileSplit) context.getInputSplit();
        Path path = split.getPath();
        k.set(path.getName()+"#"+path.getParent().getName());
        //计算该文本是CHINA的概率
        v.set("USA"+"#"+ BayesianClassification.calculateProbability(value.toString(), "USA"));
        context.write(k,v);
        //计算该文本是UK的概率
        v.set("UK"+"#"+ BayesianClassification.calculateProbability(value.toString(), "UK"));
        context.write(k,v);
    }
}

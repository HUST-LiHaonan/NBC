/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: TestTextCount
 * Author:   mac
 * Date:     2020/10/14 6:44 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Evalution;

import TextCount.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/14
 * @since 1.0.0
 */
public class TestTextCount {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "localhost");

        Job job = Job.getInstance(configuration,"TextCount");
        job.setJarByClass(TestTextCount.class);
        job.setJar("/Users/mac/IdeaProjects/NBC/target/NBC-1.0.jar");
        //设置相应的map和reduce函数
        job.setMapperClass(TestTextCountMapper.class);
        job.setReducerClass(TestTextCountReducer.class);
        //设置map输出的K-V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reduce输出的K-V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置自定义的InputFormat类
        job.setInputFormatClass(WholeFileInputFormat.class);

        FileSystem fileSystem  = FileSystem.get(new URI("hdfs://localhost:8020/"),configuration);
        //删除之前的结果集
        fileSystem.delete(new Path("/NBC/Evalution/output"),true);

        Path[] ps = new Path[]{new Path("hdfs://localhost:8020/NBC/Test/input/UK"),new Path("hdfs://localhost:8020/NBC/Test/input/CHINA")};
        FileInputFormat.setInputPaths(job,ps);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/NBC/Evalution/output"));

        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

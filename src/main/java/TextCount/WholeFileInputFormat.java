/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: WholeFileInputFormat
 * Author:   mac
 * Date:     2020/10/2 7:57 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package TextCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 〈统计文档个数〉<br>
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    /**
     * 设定是否分片
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 返回自定义的RcordReader
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //设置自定义RecordReader
        WholeRecordReader recordReader = new WholeRecordReader();
        //完成初始化
        recordReader.initialize(inputSplit,taskAttemptContext);
        return recordReader;
    }
}

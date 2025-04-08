package DSPPCode.mapreduce.warm_up.question;

import DSPPCode.mapreduce.warm_up.impl.IntSumReducerImpl;
import DSPPCode.mapreduce.warm_up.impl.TokenizerMapperImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author ikroal
 * @version 1.0.0
 * @date 2021-03-31
 * @time 15:15
 */

/**
 * Hadoop Configuration 对象是 Hadoop 中非常核心的一个配置管理类，主要用于：
 * 存储和传递作业运行时所需的所有配置信息。
 */

public class WordCountRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    // 创建一个 Job 实例，绑定当前的配置对象
    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    // 告诉 Hadoop 这个 Job 的 Jar 文件在哪
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 设置 Mapper 和 Reducer 的实现类
    job.setMapperClass(TokenizerMapperImpl.class);
    job.setReducerClass(IntSumReducerImpl.class);

    // 设置 Mapper 输出的 key 和 value 类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // 设置最终输出的 key 和 value 类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 提交 Job，并等待它完成
    return job.waitForCompletion(true) ? 0 : 1;
  }

}

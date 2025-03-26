package DSPPCode.mapreduce.semi_join.question;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.net.URISyntaxException;


public abstract class SemiJoinRunner extends Configured implements Tool {

  @Override
  public int run(String[] strings) throws Exception
  {
    Job job=Job.getInstance(getConf(),getClass().getSimpleName());
    job.setJarByClass(getClass());
    configureMapReduceTask(strings,job);
    return job.waitForCompletion(true)?0:1;
  }

  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * 1.命令行输入，输入为长度为2的字符串数组，分别表示输入路径和输出路径，
   * 例如[/tmp/mapreduce/semi_join/,/tmp/mapreduce/output/]，
   * 表明：
   * 输入路径为/tmp/mapreduce/semi_join/，在该目录下有两个文本文件SC和Course分别为学生选课信息表SC和精品课程表Course
   * 输出路径为/tmp/mapreduce/output/
   * 2.Job类，用于配置MapReduce相关具体配置
   * 功能：
   * 配置输入输出路径和Map和Reduce的相关运行信息。
   */
 abstract public void configureMapReduceTask(String []strings, Job job)
     throws IOException, URISyntaxException;
}

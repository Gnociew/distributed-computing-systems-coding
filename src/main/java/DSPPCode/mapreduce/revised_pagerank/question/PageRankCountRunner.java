package DSPPCode.mapreduce.revised_pagerank.question;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import java.io.IOException;

public abstract class PageRankCountRunner extends Configured implements Tool {

  @Override
  public int run(String[] strings) throws Exception
  {
    Job job=Job.getInstance(getConf(),getClass().getSimpleName());
    job.setJarByClass(getClass());
    // 设置数据的输入,输出路径
    FileInputFormat.addInputPath(job,new Path(strings[0]));
    FileOutputFormat.setOutputPath(job,new Path(strings[1]));
    configureMapReduceTask(job,strings);
    return job.waitForCompletion(true)?0:1;
  }

  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * 1.Job类，用于配置MapReduce相关具体配置
   * 2.命令行输入，输入为长度为2的字符串数组，分别表示输入路径和输出路径，
   * 例如[/tmp/mapreduce/revised_pagerank/input/,/tmp/mapreduce/revised_pagerank/output/]，
   * 表明输入路径为/tmp/mapreduce/revised_pagerank/，在该目录下有一个文本文件为网页信息
   *
   * 功能：
   * 配置Map和Reduce的相关运行信息。
   */
  public abstract void configureMapReduceTask(Job job,String []strings)
      throws Exception;
}
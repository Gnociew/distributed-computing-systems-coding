package DSPPCode.mapreduce.semi_join.impl;

import DSPPCode.mapreduce.semi_join.question.SemiJoinRunner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class SemiJoinRunnerImpl extends SemiJoinRunner {

  @Override
  public void configureMapReduceTask(String[] strings, Job job)
      throws IOException, URISyntaxException{

    // 将小表加载进缓存中
    job.addCacheFile(new URI(strings[0] + "/Course"));

    // 设置输入和输出路径
    FileInputFormat.setInputPaths(job, new Path(strings[0] + "/SC"));
    FileOutputFormat.setOutputPath(job, new Path(strings[1]));

    // 设置 Mapper 类
    job.setMapperClass(SemiJoinMapperImpl.class);

    // 设置 Map 输出和最终输出类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);
  }

}

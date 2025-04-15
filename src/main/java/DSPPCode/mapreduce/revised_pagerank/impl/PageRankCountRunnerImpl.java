package DSPPCode.mapreduce.revised_pagerank.impl;

import DSPPCode.mapreduce.revised_pagerank.impl.PageRankCountCombinerImpl;
import DSPPCode.mapreduce.revised_pagerank.question.PageRankCountRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankCountRunnerImpl extends PageRankCountRunner {
  @Override
  public void configureMapReduceTask(Job job,String []strings) throws Exception{
    Configuration conf = job.getConfiguration();

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(PageRankCountMapperImpl.class);
    job.setCombinerClass(PageRankCountCombinerImpl.class);
    job.setReducerClass(PageRankCountReducerImpl.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);
  }
}

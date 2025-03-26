package DSPPCode.mapreduce.revised_pagerank.question;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第一步的 Reducer
 */
public abstract class PageRankCountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
  /**
   * TODO 请完成该抽象方法
   * -
   * <p>输出： 输出文本为网页总数N
   */
  public abstract void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException;
}

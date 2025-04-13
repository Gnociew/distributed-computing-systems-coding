package DSPPCode.mapreduce.revised_pagerank.impl;

import DSPPCode.mapreduce.revised_pagerank.question.PageRankCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PageRankCountReducerImpl extends PageRankCountReducer {
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

      int totalPages = 0;

      for (IntWritable value : values) {
        totalPages += value.get();
      }

      // 输出网页总数
      context.write(new Text(String.valueOf(totalPages)), NullWritable.get());  // 只输出网页数量，key 不重要

  }
}


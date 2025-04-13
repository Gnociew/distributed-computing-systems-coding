package DSPPCode.mapreduce.revised_pagerank.impl;

import DSPPCode.mapreduce.revised_pagerank.question.PageRankCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PageRankCountMapperImpl extends PageRankCountMapper {

  private static final IntWritable ONE = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
    // System.out.println("map() called with key: " + key);  // 打印每次调用的 key
    // System.out.println("value: " + value.toString());

    // context.write(new Text("totalPages"), ONE);
    String[] values = value.toString().split(" ");
    if (values.length > 0) {
      context.write(new Text("totalPages"), ONE);
    }
  }
}

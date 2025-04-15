package DSPPCode.mapreduce.item_cf.impl;

import DSPPCode.mapreduce.item_cf.question.RecommendReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class RecommendReducerImpl extends RecommendReducer {

  private final LongWritable result = new LongWritable();

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    // System.out.println(key.toString());

    int[] row = new int[items];
    int[] col = new int[items];

    // 解析每个 value
    for (Text val : values) {
      // System.out.println(val.toString());
      String[] fields = val.toString().split("\t");

      if ("USER_SCORE".equals(fields[0])) {
        row[Integer.parseInt(fields[1]) - 1] =  Integer.parseInt(fields[2]);
      } else if ("CO_OCCURRENCE".equals(fields[0])) {
        col[Integer.parseInt(fields[1]) - 1] = Integer.parseInt(fields[2]);
      }
    }
    // System.out.println("here");

    int sum = 0;
    for (int i = 0; i < items; i++) {
      sum += row[i] * col[i];  // 逐位置相乘并累加
    }
    if (sum != 0){
      result.set(sum);
      context.write(key, result);
    }
  }
}

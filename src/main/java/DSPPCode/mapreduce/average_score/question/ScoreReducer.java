package DSPPCode.mapreduce.average_score.question;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class ScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * TODO 请完成该方法
   * <p>
   * 输出:
   * <p>
   * 每个键值对代表一门课程的班级平均分。
   * <p>
   * 例如， Mathematical analysis 80 代表数学分析的班级平均分为80分。
   */
  @Override
  public abstract void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException;

}

package DSPPCode.mapreduce.average_score.question;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class ScoreMapper extends Mapper<Object, Text, Text, IntWritable> {

  /**
   * TODO 请完成该方法
   * <p>
   * 输入:
   * <p>
   * 每个 value 代表一名学生的成绩，包含4个字符串，字符串之间按 "," 分隔，
   * 第一个字符串代表学号，随后三个字符串依次代表三门课程的分数。
   * <p>
   * 如 10160001,98,80,75 代表学号为10160001的学生,数学分析98分,概率论80分,实变函数75分。
   * <p>
   * 课程名通过 Util.getCourseName(i) 获得
   */
  @Override
  public abstract void map(Object key, Text value, Context context)
      throws IOException, InterruptedException;

}

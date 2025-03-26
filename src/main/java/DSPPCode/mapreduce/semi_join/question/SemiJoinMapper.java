package DSPPCode.mapreduce.semi_join.question;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

abstract public class SemiJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * 1.SC学生选课信息表，第一行为表字段名：S#,C#,Score（学生学号，课程ID，成绩）使用制表符隔开，剩余每一行表示一个选课记录，各个字段使用制表符隔开
   * 如：S1 C1  99 表示学号为S1的学生选修了C1课程，该课程成绩为99
   * 2.Course精品课程表，第一行为表字段名：C#,CName（课程ID，课程名）使用制表符隔开，剩余每一行表示一个课程记录，各个字段使用制表符隔开
   * 如：C1 Data Mining 表示课程ID为C1的课程名字为Data Mining
   */
  abstract public void map(LongWritable key,Text value,Context context)
      throws IOException, InterruptedException;
}

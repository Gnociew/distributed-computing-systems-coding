package DSPPCode.mapreduce.revised_pagerank.question;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第一步的 Mapper
 */
public abstract class PageRankCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * 输入文本中的每一行为一项网页信息，其组织形式为（网页名 网页排名值（出站链接...))
   * <p>
   * 如A 1.0 B D 表示一个名称为A的网页，当前排名值为1.0,该网页链接至名称为B和D的网页（所有网页权重默认为1.0）
   */
  public abstract void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException;
}

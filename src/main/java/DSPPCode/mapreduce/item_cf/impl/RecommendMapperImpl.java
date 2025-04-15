package DSPPCode.mapreduce.item_cf.impl;

import DSPPCode.mapreduce.item_cf.question.RecommendMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class RecommendMapperImpl extends RecommendMapper {

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // System.out.println(fileName);
    // System.out.println(key.toString());
    // System.out.println(value.toString());

    // 分析当前读取的文件名（用户评分矩阵 or 同现矩阵）
    if (fileName.equals(userScoreMatrixFileName)) {
      // 处理用户评分矩阵
      processUserScoreMatrix(value, context);
    } else if (fileName.equals(coOccurrenceMatrixFileName)) {
      // 处理同现矩阵
      processCoOccurrenceMatrix(value, context);
    }
  }

  // 处理用户评分矩阵
  private void processUserScoreMatrix(Text value, Context context) throws IOException, InterruptedException {
    // 假设输入格式为：用户编号 物品编号 评分
    String[] fields = value.toString().split("\t");
    int userId = Integer.parseInt(fields[0]);
    int itemId = Integer.parseInt(fields[1]);
    int rating = Integer.parseInt(fields[2]);

    // 为同现矩阵的每一列分发行数据
    for  (int i = 1; i <= items; i++) {
      context.write(new Text(userId + "\t" + i), new Text("USER_SCORE\t" + itemId + "\t" + rating));
    }
  }

  // 处理同现矩阵
  private void processCoOccurrenceMatrix(Text value, Context context) throws IOException, InterruptedException {
    // 假设输入格式为：物品编号 物品编号 共同喜欢的用户数
    String[] fields = value.toString().split("\t");
    int itemId1 = Integer.parseInt(fields[0]);
    int itemId2 = Integer.parseInt(fields[1]);
    int coOccurrenceCount = Integer.parseInt(fields[2]);

    // 为评分矩阵的每一行分发列数据
    for  (int i = 1; i <= users; i++) {
      context.write(new Text(i + "\t" + itemId1), new Text("CO_OCCURRENCE\t" + itemId2 + "\t" + coOccurrenceCount));
    }
  }
}

package DSPPCode.mapreduce.item_cf.impl;

import DSPPCode.mapreduce.item_cf.question.CoOccurrenceMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class CoOccurrenceMapperImpl extends CoOccurrenceMapper {


  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // System.out.println(key.toString());
    // System.out.println(value.toString());

    // 输入格式：用户编号，物品编号，评分
    String[] fields = value.toString().split("\t");
    int userId = Integer.parseInt(fields[0]);
    int itemId = Integer.parseInt(fields[1]);
    int rating = Integer.parseInt(fields[2]);

    // System.out.println("CoOccurrenceMapperImpl");
    // System.out.println(userId);
    // System.out.println(itemId);

    if (rating > 0) {
      context.write(new Text(String.valueOf(userId)), new Text(String.valueOf(itemId)));
    }
  }
}

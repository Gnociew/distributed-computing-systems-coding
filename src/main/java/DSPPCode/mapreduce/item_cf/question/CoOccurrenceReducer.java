package DSPPCode.mapreduce.item_cf.question;

import DSPPCode.mapreduce.item_cf.question.utils.Constants;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

abstract public class CoOccurrenceReducer extends Reducer<Text, Text, Text, LongWritable> {
  // 用户数
  protected int users;
  // 物品数
  protected int items;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    users = context.getConfiguration().getInt(Constants.USER_COUNT, 0);
    items = context.getConfiguration().getInt(Constants.ITEM_COUNT, 0);
  }

  /**
   * TODO 请完成该抽象方法
   * <p>Step1 Reducer</p>
   * <p>
   *   输出: 同现矩阵
   * </p>
   */
  @Override
  abstract public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException;
}

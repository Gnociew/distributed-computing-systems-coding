package DSPPCode.mapreduce.item_cf.question;

import DSPPCode.mapreduce.item_cf.question.utils.Constants;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

abstract public class CoOccurrenceMapper extends Mapper<Object, Text, Text, Text> {
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
   * <p>Step1 Mapper</p>
   * <p>
   *   输入: 用户评分矩阵
   * </p>
   */
  @Override
  abstract public void map(Object key, Text value, Context context) throws IOException, InterruptedException;
}

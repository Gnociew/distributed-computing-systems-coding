package DSPPCode.mapreduce.item_cf.question;

import DSPPCode.mapreduce.item_cf.question.utils.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

abstract public class RecommendMapper extends Mapper<Object, Text, Text, Text> {
  // 用户数
  protected int users;
  // 物品数
  protected int items;
  // 用户评分矩阵文件名
  protected String userScoreMatrixFileName;
  // 同现矩阵文件名
  protected String coOccurrenceMatrixFileName;
  // 当前读取的文件名
  protected String fileName;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    users = context.getConfiguration().getInt(Constants.USER_COUNT, 0);
    items = context.getConfiguration().getInt(Constants.ITEM_COUNT, 0);
    userScoreMatrixFileName = context.getConfiguration().get(Constants.USER_SCORE_MATRIX_FILE_NAME, "");
    coOccurrenceMatrixFileName = context.getConfiguration().get(Constants.CO_OCCURRENCE_MATRIX_FILE_NAME, "");
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    fileName = fileSplit.getPath().getParent().getName();
  }

  /**
   * TODO 请完成该抽象方法
   * <p>Step2 Mapper</p>
   * <p>
   *   输入:
   *   <li>用户评分矩阵</li>
   *   <li>同现矩阵，来自 {@link DSPPCode.mapreduce.item_cf.question.CoOccurrenceReducer} 的输出</li>
   * </p>
   */
  @Override
  abstract protected void map(Object key, Text value, Context context) throws IOException, InterruptedException;
}

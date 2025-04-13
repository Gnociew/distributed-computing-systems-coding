package DSPPCode.mapreduce.revised_pagerank.question;

import DSPPCode.mapreduce.revised_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第二步的 Mapper 已经给出，请完成与之配合的 Reducer
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, ReducePageRankWritable> {
  /**
   * 输入：
   * 输入文本中的每一行为一项网页信息，其组织形式为（网页名 网页排名值（出站链接...))
   * <p>
   * 如 A 1.0 B D 表示一个名称为A的网页，当前排名值为1.0,该网页链接至名称为B和D的网页（所有网页权重默认为1.0）
   * <p>
   * 可借助ReducePageRankWritable类来实现
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 以空格为分隔符切分
    String[] pageInfo = value.toString().split(" ");
    // 网页的排名值
    double pageRank = Double.parseDouble(pageInfo[1]);
    // 网页的出站链接数
    int outLink = pageInfo.length - 2;

    ReducePageRankWritable writable;
    writable = new ReducePageRankWritable();
    // 计算贡献值并保存
    writable.setData(String.valueOf(pageRank / outLink));
    // 设置对应标识
    writable.setTag(ReducePageRankWritable.PR_L);

    // 对于每一个出站链接，输出贡献值
    for (int i = 2; i < pageInfo.length; i++) {
      context.write(new Text(pageInfo[i]), writable);
    }
    writable = new ReducePageRankWritable();
    // 保存网页信息并标识
    writable.setData(value.toString());
    writable.setTag(ReducePageRankWritable.PAGE_INFO);
    // 以输入的网页信息的网页名称为key进行输出
    context.write(new Text(pageInfo[0]), writable);
  }
}

// package DSPPCode.mapreduce.revised_pagerank.question;
//
// import DSPPCode.mapreduce.revised_pagerank.question.utils.ReducePageRankWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;
//
// import java.io.IOException;
//
// public class PageRankMapper extends Mapper<LongWritable, Text, Text, ReducePageRankWritable> {
//
//   // 可复用对象，减少 GC 压力
//   private final ReducePageRankWritable prWritable = new ReducePageRankWritable();
//   private final ReducePageRankWritable structureWritable = new ReducePageRankWritable();
//   private final Text outKey = new Text();
//
//   @Override
//   public void map(LongWritable key, Text value, Context context)
//       throws IOException, InterruptedException {
//
//     // A 1.0 B D
//     String[] pageInfo = value.toString().split(" ");
//     String pageName = pageInfo[0];
//     double pageRank = Double.parseDouble(pageInfo[1]);
//     int outLink = pageInfo.length - 2;
//
//     // 防止除以 0（虽然数据集不该这样）
//     if (outLink > 0) {
//       double contribution = pageRank / outLink;
//
//       // 设置贡献值 PR_L
//       prWritable.setTag(ReducePageRankWritable.PR_L);
//       prWritable.setPrValue(contribution);
//
//       for (int i = 2; i < pageInfo.length; i++) {
//         outKey.set(pageInfo[i]);
//         context.write(outKey, prWritable);  // 发给每个出链
//       }
//     }
//
//     // 设置结构信息 PAGE_INFO
//     structureWritable.setTag(ReducePageRankWritable.PAGE_INFO);
//     structureWritable.setPageInfo(value.toString());
//     outKey.set(pageName);
//     context.write(outKey, structureWritable);  // 发自己结构
//   }
// }

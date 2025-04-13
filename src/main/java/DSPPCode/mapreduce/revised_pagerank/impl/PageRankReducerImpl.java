package DSPPCode.mapreduce.revised_pagerank.impl;

import DSPPCode.mapreduce.revised_pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.revised_pagerank.question.PageRankRunner;
import DSPPCode.mapreduce.revised_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer {

  // 阻尼系数
  private static final double DAMPING_FACTOR = 0.85;

  @Override
  public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
      throws IOException, InterruptedException {

    double prSum = 0.0;
    String pageInfo = null;

    // 解析所有 value
    for (ReducePageRankWritable val : values) {
      if (ReducePageRankWritable.PR_L.equals(val.getTag())) {
        prSum += Double.parseDouble(val.getData());  // 累加贡献值
      } else if (ReducePageRankWritable.PAGE_INFO.equals(val.getTag())) {
        pageInfo = val.getData();  // 保存网页结构
      }
    }

    if (pageInfo == null) return; // 安全处理

    // 获取网页总数 N 和当前迭代轮数
    int totalPage = context.getConfiguration().getInt(PageRankRunner.TOTAL_PAGE, 1);
    int iteration = context.getConfiguration().getInt(PageRankRunner.ITERATION, 0);

    // 新的 PageRank 值：阻尼系数公式
    double newPageRank = (1 - DAMPING_FACTOR) / totalPage + DAMPING_FACTOR * prSum;

    // 从原网页结构中提取旧的 PageRank 值
    String[] parts = pageInfo.split(" ");
    double oldPageRank = Double.parseDouble(parts[1]);

    // 判断是否收敛：绝对误差 < DELTA
    if (Math.abs(oldPageRank - newPageRank) < PageRankRunner.DELTA) {
      context.getCounter(PageRankRunner.GROUP_NAME, PageRankRunner.COUNTER_NAME).increment(1);  // 全部网页都收敛时停止迭代
    }

    // 构造新的输出内容：网页名 + 新排名 + 出链
    StringBuilder builder = new StringBuilder();
    builder.append(parts[0]).append(" ").append(newPageRank);
    for (int i = 2; i < parts.length; i++) {
      builder.append(" ").append(parts[i]);
    }

    context.write(new Text(builder.toString()), NullWritable.get());
  }
}
// package DSPPCode.mapreduce.revised_pagerank.impl;
//
// import DSPPCode.mapreduce.revised_pagerank.question.PageRankReducer;
// import DSPPCode.mapreduce.revised_pagerank.question.PageRankRunner;
// import DSPPCode.mapreduce.revised_pagerank.question.utils.ReducePageRankWritable;
// import org.apache.hadoop.io.NullWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Reducer;
//
// import java.io.IOException;
//
// public class PageRankReducerImpl extends PageRankReducer {
//
//   private static final double DAMPING_FACTOR = 0.85;
//
//   private final Text outKey = new Text();
//
//   @Override
//   public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
//       throws IOException, InterruptedException {
//
//     double prSum = 0.0;
//     String pageInfo = null;
//
//     for (ReducePageRankWritable val : values) {
//       if (val.getTag() == ReducePageRankWritable.PR_L) {
//         prSum += val.getPrValue();
//       } else if (val.getTag() == ReducePageRankWritable.PAGE_INFO) {
//         pageInfo = val.getPageInfo();
//       }
//     }
//
//     if (pageInfo == null) return;
//
//     int totalPage = context.getConfiguration().getInt(PageRankRunner.TOTAL_PAGE, 1);
//
//     double newPageRank = (1 - DAMPING_FACTOR) / totalPage + DAMPING_FACTOR * prSum;
//
//     String[] parts = pageInfo.split(" ");
//     double oldPageRank = Double.parseDouble(parts[1]);
//
//     if (Math.abs(oldPageRank - newPageRank) < PageRankRunner.DELTA) {
//       context.getCounter(PageRankRunner.GROUP_NAME, PageRankRunner.COUNTER_NAME).increment(1);
//     }
//
//     StringBuilder builder = new StringBuilder();
//     builder.append(parts[0]).append(" ").append(newPageRank);
//     for (int i = 2; i < parts.length; i++) {
//       builder.append(" ").append(parts[i]);
//     }
//
//     outKey.set(builder.toString());
//     context.write(outKey, NullWritable.get());
//   }
// }

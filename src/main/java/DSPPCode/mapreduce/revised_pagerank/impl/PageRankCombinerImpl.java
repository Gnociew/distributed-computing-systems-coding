//package DSPPCode.mapreduce.revised_pagerank.impl;
//
//import DSPPCode.mapreduce.revised_pagerank.question.utils.ReducePageRankWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//
//public class PageRankCombinerImpl extends Reducer<Text, ReducePageRankWritable, Text, ReducePageRankWritable> {
//
//  private final ReducePageRankWritable outWritable = new ReducePageRankWritable();
//
//  @Override
//  public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
//      throws IOException, InterruptedException {
//
//    double prSum = 0.0;
//    boolean hasPR_L = false;
//
//    for (ReducePageRankWritable val : values) {
//      if (ReducePageRankWritable.PR_L.equals(val.getTag())) {
//        prSum += Double.parseDouble(val.getData());
//        hasPR_L = true;
//      } else if (ReducePageRankWritable.PAGE_INFO.equals(val.getTag())) {
//        // 结构信息不能聚合，必须保留原始输出
//        context.write(key, val);
//      }
//    }
//
//    if (hasPR_L) {
//      outWritable.setTag(ReducePageRankWritable.PR_L);
//      outWritable.setData(String.valueOf(prSum));
//      context.write(key, outWritable);
//    }
//  }
//}

// package DSPPCode.mapreduce.revised_pagerank.impl;
//
// import DSPPCode.mapreduce.revised_pagerank.question.utils.ReducePageRankWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Reducer;
//
// import java.io.IOException;
//
// public class PageRankCombinerImpl extends Reducer<Text, ReducePageRankWritable, Text, ReducePageRankWritable> {
//
//   private final ReducePageRankWritable prWritable = new ReducePageRankWritable();
//
//   @Override
//   public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
//       throws IOException, InterruptedException {
//
//     System.out.println("Combiner called for key = " + key.toString());
//
//     double prSum = 0.0;
//     boolean hasPR_L = false;
//
//     for (ReducePageRankWritable val : values) {
//       if (val.getTag() == ReducePageRankWritable.PR_L) {
//         prSum += val.getPrValue();
//         hasPR_L = true;
//       } else if (val.getTag() == ReducePageRankWritable.PAGE_INFO) {
//         // 网页结构信息不能聚合，直接传给 Reducer
//         context.write(key, val);
//       }
//     }
//
//     if (hasPR_L) {
//       prWritable.setTag(ReducePageRankWritable.PR_L);
//       prWritable.setPrValue(prSum);
//       context.write(key, prWritable);
//     }
//   }
// }

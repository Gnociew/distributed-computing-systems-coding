package DSPPCode.mapreduce.item_cf.impl;

import DSPPCode.mapreduce.item_cf.question.CoOccurrenceReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CoOccurrenceReducerImpl extends CoOccurrenceReducer {

  private final LongWritable result = new LongWritable();
  // 使用一个全局变量来存储数据
  private final Map<Text, LongWritable> outputData = new HashMap<>();

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    // System.out.println("CoOccurrenceReducerImpl");
    // System.out.println(key.toString());

    Set<Integer> items = new HashSet<>();

    // 将当前用户评分过的物品编号加入 items 集合
    for (Text val : values) {
      // System.out.println(val.toString());
      items.add(Integer.parseInt(val.toString()));
    }

    // 把每个物品对及其值存储到全局变量中
    for (Integer item1 : items) {
      Text pair = new Text(item1 + "\t" + item1);
      LongWritable currentValue = outputData.get(pair);
      // 若该 item 对出现过
      if (currentValue != null) {
        outputData.put(pair, new LongWritable(currentValue.get() + 1));
      } else {
        outputData.put(pair, new LongWritable(1));
      }

      for (Integer item2 : items) {
        if (item1 != item2) {
          Text pair1 = new Text(item1 + "\t" + item2);
          LongWritable currentValue1 = outputData.get(pair1);
          if (currentValue1 != null) {
            outputData.put(pair1, new LongWritable(currentValue1.get() + 1));
          } else {
            outputData.put(pair1, new LongWritable(1));
          }
        }
      }
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // 在 cleanup 中，合并所有相同的键，并输出结果
    for (Map.Entry<Text, LongWritable> entry : outputData.entrySet()) {
      Text key = entry.getKey();
      // System.out.println("key "+key.toString());
      LongWritable value = entry.getValue();
      // System.out.println("value "+value.toString());

      context.write(key, value);
    }
  }
}

package DSPPCode.spark.knn.impl;

import DSPPCode.spark.knn.question.Data;
import DSPPCode.spark.knn.question.KNN;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import java.util.ArrayList;
import java.util.List;

import java.util.*;
public class KNNImpl extends KNN {

  public KNNImpl(int k) {
    super(k);
  }


  @Override
  public JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
    // return queryData.cartesian(trainData);

    // 广播训练数据
    /* collect() 会将分布式的数据拉取到单个机器上（驱动程序），并生成一个完整的本地集合。    */
    /* 通过 JavaRDD<Data> 对象的 context() 方法可以获取到与其相关联的 SparkContext     */
    /* 每个 worker 节点都可以通过广播变量 broadcastTrainData 的 value() 方法访问到数据  */
    Broadcast<List<Data>> broadcastTrainData = JavaSparkContext.fromSparkContext(trainData.context()).broadcast(trainData.collect());

    // 对每条查询数据，计算与所有训练数据的距离
    return queryData.flatMapToPair(query -> {
      List<Tuple2<Data, Data>> results = new ArrayList<>();
      for (Data trainDataPoint : broadcastTrainData.value()) {
        results.add(new Tuple2<>(query, trainDataPoint));
      }
      return results.iterator();
    });
  }


  @Override
  public JavaPairRDD<Integer, Tuple2<Integer, Double>> calculateDistance(JavaPairRDD<Data, Data> data) {
    return data.mapToPair(pair -> {
      // 获取查询数据和训练数据
      Data queryData = pair._1();
      Data trainData = pair._2();

      // 计算欧式距离
      double distance = 0.0;
      for (int i = 0; i < queryData.x.length; i++) {
        distance += Math.pow(queryData.x[i] - trainData.x[i], 2);
      }
      distance = Math.sqrt(distance);

      // 返回（查询数据的id， (训练数据的类别id, 距离)）
      return new Tuple2<>(queryData.id, new Tuple2<>(trainData.y, distance));
    });
  }

  @Override
  public JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> distances) {
    // 使用 combineByKey 聚合距离数据
    JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> combinedDistances = distances.combineByKey(
        // 初始化，包含第一个值的列表
        value -> new ArrayList<>(Collections.singletonList(value)),
        // 合并同一个分区内同一个键的值，将每个新值添加到列表中
        (list, value) -> {
          list.add(value);
          return list;
        },
        // 合并不同分区中的同一个键的值，合并两个列表
        (list1, list2) -> {
          list1.addAll(list2);
          return list1;
        }
    );

    // 对每个 queryId 进行 KNN 分类
    return combinedDistances.mapToPair(group -> {
      Integer queryId = group._1();  // 获取 queryId
      List<Tuple2<Integer, Double>> neighbors = group._2();  // 获取邻居数据

      // 按距离升序排序邻居，获取 K 个最近邻，并计算标签的频次
      neighbors.sort(Comparator.comparingDouble(Tuple2::_2));
      Map<Integer, Integer> labelCount = getTopKLabels(neighbors);

      // 获取最频繁的标签
      int mostFrequentLabel = labelCount.entrySet().stream()
          .max(Comparator.comparingInt(Map.Entry<Integer, Integer>::getValue)
              .thenComparing(Map.Entry::getKey))  // 如果频次相同，选择标签值小的
          .map(Map.Entry::getKey)
          .orElse(-1);  // 如果没有邻居，返回 -1

      return new Tuple2<>(queryId, mostFrequentLabel);
    });
  }

  // 获取前 k 个邻居的标签频率
  private Map<Integer, Integer> getTopKLabels(List<Tuple2<Integer, Double>> neighbors) {
    Map<Integer, Integer> labelCount = new HashMap<>();
    for (int i = 0; i < Math.min(k, neighbors.size()); i++) {
      int label = neighbors.get(i)._1();
      labelCount.put(label, labelCount.getOrDefault(label, 0) + 1);
    }
    return labelCount;
  }

}

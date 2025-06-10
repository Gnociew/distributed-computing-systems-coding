package DSPPCode.spark.revised_kmeans.impl;

import DSPPCode.spark.revised_kmeans.question.RevisedKmeans;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RevisedKmeansImpl extends RevisedKmeans {

  @Override
  public Integer closestPoint(List<Integer> p, Broadcast<List<List<Double>>> kPoints) {
    List<List<Double>> centers = kPoints.value();
    int minIndex = -1;
    double minDist = Double.MAX_VALUE;

    // 将p转换为Double类型的列表以便计算距离
    List<Double> pDouble = new ArrayList<>();
    for (Integer num : p) {
      pDouble.add(num.doubleValue());
    }

    // 遍历所有聚类中心，计算距离并找到最近的
    for (int i = 0; i < centers.size(); i++) {
      double dist = distanceSquared(pDouble, centers.get(i));
      if (dist < minDist) {
        minDist = dist;
        minIndex = i;
      }
    }
    return minIndex;
  }

  @Override
  // 本地变量 -> 所有 Spark 的工作节点
  public Broadcast<List<List<Double>>> createBroadcastVariable(JavaSparkContext sc, List<List<Double>> localVariable) {
    /* JavaSparkContext 对象：与 Spark 集群交互的入口点 */
    return sc.broadcast(localVariable);
  }

  @Override
  public List<List<Double>> iterationStep(JavaRDD<List<Integer>> points, Broadcast<List<List<Double>>> broadcastKPoints) {
    List<List<Double>> oldCenters = broadcastKPoints.value();
    /*（键：数据点 p 所属的聚类中心的索引，值：([p的坐标]，1)）*/
    JavaPairRDD<Integer, Tuple2<List<Integer>, Integer>> closest = getClosestPoint(points, broadcastKPoints);
    // 得到新的聚类中心
    List<List<Double>> newCenters = getNewPoints(closest);

    // 检查新旧聚类中心的数量是否一致，避免错误
    if (newCenters.size() != oldCenters.size()) {
      return newCenters;
    }

    // 判断所有聚类中心是否收敛（变化小于阈值）
    boolean converged = true;
    for (int i = 0; i < oldCenters.size(); i++) {
      double dist = Math.sqrt(distanceSquared(oldCenters.get(i), newCenters.get(i)));
      // 只要有一个不收敛就是不收敛
      if (dist >= DELTA) {
        converged = false;
        break;
      }
    }

    return converged ? Collections.emptyList() : newCenters;
  }
}

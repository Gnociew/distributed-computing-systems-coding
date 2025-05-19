package DSPPCode.spark.pi.question;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public abstract class PiSimulator implements Serializable {

  private static final String MODE = "local";

  public double run(int slices) {
    JavaSparkContext sc = new JavaSparkContext(MODE, "Pi");
    // 生成n个”子弹“，用于随后的打点操作
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }
    // 转换为一个 Spark 的分布式数据集，并将其分为多个分区进行并行处理
    JavaRDD<Integer> parallelInput = sc.parallelize(l, slices);
    // reduce 操作依次将 RDD 中的元素两两进行合并，最终得到一个单一的结果
    int count = sampledPoint(parallelInput).reduce((Integer i1, Integer i2) -> (i1 + i2));
    double pi = 4.0 * count / n;
    sc.close();
    return pi;
  }

  /**
   * TODO 请完成该方法
   * <p>
   * 利用之前生成的”子弹“，进行打点操作，即判断每个点是否落在圆内
   * @param parallelInput 生成的”子弹“
   * @return 返回一个RDD，记录每个点是否落在圆内（在圆内：1，不在圆内：0）
   */
  public abstract JavaRDD<Integer> sampledPoint(JavaRDD<Integer> parallelInput);

}

package DSPPCode.spark.pi.impl;

import DSPPCode.spark.pi.question.PiSimulator;
import org.apache.spark.api.java.JavaRDD;

public class PiSimulatorImpl extends PiSimulator {

  @Override
  public JavaRDD<Integer> sampledPoint(JavaRDD<Integer> parallelInput) {
    return parallelInput.map(i -> {
      // 生成两个 0~1 的随机数（模拟点在 [0, 1] × [0, 1] 的正方形内）
      double x = Math.random();
      double y = Math.random();
      // 判断是否在单位圆内（Pi*R^2 = 0.25Pi = 1/4 * 1 * Pi）
      return (x * x + y * y <= 1) ? 1 : 0;
    });
  }
}

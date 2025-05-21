package DSPPCode.spark.logistic_regression.impl;

import DSPPCode.spark.logistic_regression.question.DataPoint;
import DSPPCode.spark.logistic_regression.question.IterationStep;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class IterationStepImpl extends IterationStep {

  @Override
  public boolean termination(double[] old, double[] newWeight) {
    double sum = 0.0;
    for (int i = 0; i < old.length; i++) {
      double diff = newWeight[i] - old[i];
      sum += diff * diff;
    }
    return sum < THRESHOLD;
  }

  @Override
  public double[] runStep(JavaRDD<DataPoint> points, double[] weights) {
    // 对于每个数据点，计算其梯度
    JavaRDD<double[]> gradients = points.map(new ComputeGradientImpl(weights));
    // 合并成总梯度（递归地将两个梯度数组合并）
    double[] totalGradient = gradients.reduce(new VectorSumImpl());

    // 更新梯度
    double[] newWeights = new double[weights.length];
    for (int i = 0; i < weights.length; i++) {
      newWeights[i] = weights[i] + STEP * totalGradient[i];
    }
    return newWeights;
  }

  public static class ComputeGradientImpl extends ComputeGradient {
    public ComputeGradientImpl(double[] weights) {
      super(weights);
    }

    @Override
    /*call 函数并不会在类初始化时自动执行，而是在 Spark 操作执行时被触发*/
    public double[] call(DataPoint dataPoint) throws Exception {
      double[] x = dataPoint.x;
      double y = dataPoint.y;

      double dotProduct = 0.0;
      for (int i = 0; i < weights.length; i++) {
        dotProduct += weights[i] * x[i];
      }

      double h = 1.0 / (1.0 + Math.exp(-dotProduct));
      double[] gradient = new double[weights.length];
      for (int i = 0; i < weights.length; i++) {
        gradient[i] = (y - h) * x[i];
      }

      return gradient;
    }
  }

  public static class VectorSumImpl extends VectorSum {
    @Override
    public double[] call(double[] a, double[] b) throws Exception {
      double[] sum = new double[a.length];
      for (int i = 0; i < a.length; i++) {
        sum[i] = a[i] + b[i];
      }
      return sum;
    }
  }
}
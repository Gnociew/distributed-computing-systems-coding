package DSPPTest.student.spark.pi;

import DSPPCode.spark.pi.impl.PiSimulatorImpl;
import DSPPTest.student.TestTemplate;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PiTest extends TestTemplate {

  @Test(timeout = 5000)
  public void test1() {
    PiSimulatorImpl piSimulatorImpl = new PiSimulatorImpl();
    // 设定打点数量为100000
    double pi = piSimulatorImpl.run(1);
    //限定误差范围±0.05
    assertEquals(pi, Math.PI, 0.05);
    System.out.println("case1:估计的Pi值在误差范围±0.05内，恭喜通过~");
  }

  @Test(timeout = 15000)
  public void test2() {
    PiSimulatorImpl piSimulatorImpl = new PiSimulatorImpl();
    // 设定打点数量为10000000
    double pi = piSimulatorImpl.run(100);
    //限定误差范围±0.005
    assertEquals(pi, Math.PI, 0.005);
    System.out.println("case2:估计的Pi值在误差范围±0.005内，恭喜通过~");
  }
}

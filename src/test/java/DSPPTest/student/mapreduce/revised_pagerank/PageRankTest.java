package DSPPTest.student.mapreduce.revised_pagerank;

import DSPPCode.mapreduce.revised_pagerank.impl.PageRankCountRunnerImpl;
import DSPPCode.mapreduce.revised_pagerank.question.PageRankRunner;
import DSPPTest.student.TestTemplate;
import DSPPTest.util.Parser.KVParser;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;
import static DSPPTest.util.Verifier.verifyList;

public class PageRankTest extends TestTemplate {

  @Test(timeout = 40000)
  public void test() throws Exception {
    // 设置路径
    String inputPath = root + "/mapreduce/revised_pagerank/input";
    String outputPath = outputRoot + "/mapreduce/revised_pagerank";

    String outputPath1 = outputPath + "/step_one";
    String outputFile1 = outputPath1 + "/part-r-00000";
    String outputPath2 = outputPath + "/step_two/";
    String outputFile2 = outputPath2 + "19/part-r-00000";

    String answerFile1 = root + "/mapreduce/revised_pagerank/answer-step1";
    String answerFile2 = root + "/mapreduce/revised_pagerank/answer-step2";

    // 删除旧的输出
    deleteFolder(outputPath);

    // 执行
    String[] args1 = {inputPath, outputPath1};
    ToolRunner.run(new PageRankCountRunnerImpl(), args1);
    String[] args2 = {inputPath, outputPath2, outputFile1};
    PageRankRunner pageRank = new PageRankRunner();
    pageRank.mainRun(args2);

    // 检验结果
    try {
      verifyList(readFile2String(outputFile1), readFile2String(answerFile1));
      System.out.println("Count 结果正确");
    } catch (Throwable t) {
      System.out.println("Count 结果错误.");
    }
    try {
      verifyKV(readFile2String(outputFile2), readFile2String(answerFile2), new KVParser(" "));
      System.out.println("PageRank 结果正确");
    } catch (Throwable t) {
      System.out.println("PageRank 结果错误.");
    }
  }
}

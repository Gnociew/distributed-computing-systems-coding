package DSPPTest.student.mapreduce.item_cf;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.FileOperator.readFolder2StringExcludeHiddenFiles;
import static DSPPTest.util.Verifier.verifyList;

import DSPPCode.mapreduce.item_cf.question.CoOccurrenceRunner;
import DSPPCode.mapreduce.item_cf.question.RecommendRunner;
import DSPPTest.student.TestTemplate;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class ItemCFTest extends TestTemplate {
  // 同现矩阵
  private static final String inputPath1 = root + "/mapreduce/item_cf/input";
  private static final String outputPath1 = outputRoot + "/mapreduce/item_cf/co_occurrence";
  private static final String answerFile1 = root + "/mapreduce/item_cf/co_occurrence";

  // 推荐矩阵
  private static final String inputPath2 = answerFile1;
  private static final String outputPath2 = outputRoot + "/mapreduce/item_cf/output";
  private static final String answerFile2 = root + "/mapreduce/item_cf/answer";

  // 测试Step1，同现矩阵
  @Test
  public void test1() throws Exception {
    deleteFolder(outputPath1);

    String[] args = new String[]{"3", "3", inputPath1, outputPath1};
    int exitCode = ToolRunner.run(new CoOccurrenceRunner(), args);
    checkErr(exitCode);
    verifyList(readFolder2StringExcludeHiddenFiles(outputPath1), readFile2String(answerFile1));

    System.out.println("恭喜通过~");
  }

  // 测试Step2，推荐矩阵
  @Test
  public void test2() throws Exception {
    deleteFolder(outputPath2);

    String[] args = new String[]{"input", "item_cf", "3", "3", inputPath1, inputPath2, outputPath2};
    int exitCode = ToolRunner.run(new RecommendRunner(), args);
    checkErr(exitCode);
    verifyList(readFolder2StringExcludeHiddenFiles(outputPath2), readFile2String(answerFile2));

    System.out.println("恭喜通过~");
  }

  private void checkErr(int exitCode) {
    if (exitCode != 0) {
      System.out.println("程序出错");
    }
  }
}

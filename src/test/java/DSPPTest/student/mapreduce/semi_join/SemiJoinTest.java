package DSPPTest.student.mapreduce.semi_join;

import DSPPCode.mapreduce.semi_join.impl.SemiJoinRunnerImpl;
import DSPPCode.mapreduce.semi_join.question.SemiJoinRunner;
import DSPPTest.student.TestTemplate;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import java.io.File;
import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.FileOperator.readFolder2StringExcludeHiddenFiles;
import static DSPPTest.util.Verifier.verifyList;


public class SemiJoinTest extends TestTemplate {

  @Test
  public void test() throws Exception {
    //set dir
    String inputFolder = root + "/mapreduce/semi_join";
    String outputFolder = outputRoot + "/mapreduce/semi_join";
    String outputSuccessFile = outputFolder + "/_SUCCESS";
    String answerFile = root + "/mapreduce/semi_join/answer";

    //delete old dir
    deleteFolder(outputFolder);
    SemiJoinRunner semiJoinRunner = new SemiJoinRunnerImpl();
    //do
    String [] args = {inputFolder,outputFolder};
    int exitCode = ToolRunner.run(semiJoinRunner,args);
    File successFile = new File(outputSuccessFile);
    if (!successFile.delete()){
      System.err.println("不能删除Success文件");
    }

    // String actualOutput = readFolder2StringExcludeHiddenFiles(outputFolder);
    // System.out.println("Mapper 输出结果：");
    // System.out.println(actualOutput);

    verifyList(readFolder2StringExcludeHiddenFiles(outputFolder), readFile2String(answerFile));
    System.out.println("恭喜通过~");
    System.exit(exitCode);
  }
}

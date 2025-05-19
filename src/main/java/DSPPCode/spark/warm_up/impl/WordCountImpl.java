package DSPPCode.spark.warm_up.impl;

import DSPPCode.spark.warm_up.question.WordCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Java 答案示例
 */
public class WordCountImpl extends WordCount {

  @Override
  public JavaPairRDD<String, Integer> wordCount(JavaRDD<String> lines) {
    // 将每行内容按分隔符拆分成单个单词
    /*
    * flatMap 会将 RDD 中的每一行（或每个元素）依次传递给 lambda 表达式
    * (String line) -> Arrays.asList(line.split(" ")) 是 lambda 表达式
    * （String line）：表示传入的每一行文本
    * */
    JavaRDD<String> words = lines.flatMap((String line)
        -> Arrays.asList(line.split(" ")).iterator());

    // 将每个单词的频数设置为1，即将每个单词映射为[单词, 1]
    // Tuple2 是一个包含两个元素的容器，它存储了一个键值对
    JavaPairRDD<String, Integer> wordPairs = words
        .mapToPair((String word) -> new Tuple2<>(word, 1));

    // 返回累加计数结果
    // Integer::sum 是 Java 中的一个方法引用，表示使用 Integer 类型的 sum 方法来合并值
    return wordPairs.reduceByKey(Integer::sum);
  }

}

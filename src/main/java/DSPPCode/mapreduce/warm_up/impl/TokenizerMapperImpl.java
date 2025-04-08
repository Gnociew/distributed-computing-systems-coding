package DSPPCode.mapreduce.warm_up.impl;

import DSPPCode.mapreduce.warm_up.question.TokenizerMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * 答案示例
 */
public class TokenizerMapperImpl extends TokenizerMapper {

  /// 要输出的值 <word, 1> 中的 1。
  /// IntWritable 是 Hadoop 定义的可序列化（把内存中的对象转换成字节序列）整数类型，不能用原始的 int。
  /// 用 static final 是为了只创建一次对象，避免频繁创建和销毁，提高效率

  /// 为什么需要序列化？
   /// 因为 Hadoop 是 分布式计算框架，Mapper 和 Reducer 不一定在同一台机器上运行，所以：
   /// Mapper 的输出 <key, value> 需要 通过网络传输 发送给 Reducer。
   /// Reducer 处理完结果，也要 写入分布式文件系统（如 HDFS）。
   /// 这两个过程都需要把数据对象转换成字节，即序列化
  private static final IntWritable ONE = new IntWritable(1);

  /// Text 是 Hadoop 的可序列化字符串类型；每个词处理后，会用这个变量来 set() 成新值，然后输出。
  private final Text word = new Text();

  /// Pattern 是 Java 的正则类。
  /// \\W+ 表示匹配 非单词字符（比如标点、空格等），用来清洗文本，只保留字母数字
  private final Pattern pattern = Pattern.compile("\\W+");

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    /// 参数：
    ///key：当前行的偏移量
    ///value：当前行的内容
    ///context：Hadoop 提供的上下文对象，用来输出结果。

    ///  把 value 转换成字符串，并用空格默认切分成单词
    StringTokenizer itr = new StringTokenizer(value.toString());
    ///  遍历每一个词
    while (itr.hasMoreTokens()) {
      String str = itr.nextToken();
      str = pattern.matcher(str).replaceAll(""); /// 用正则表达式清洗掉所有非单词字符

      ///  把这个词放入 Text word 对象，然后写出一对键值对
      word.set(str);
      context.write(word, ONE);
    }
  }

}

package DSPPCode.mapreduce.semi_join.impl;

import DSPPCode.mapreduce.semi_join.question.SemiJoinMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class SemiJoinMapperImpl extends SemiJoinMapper {

  // 缓存 Course 表中的所有课程号 C#
  private final Set<String> courseSet = new HashSet<>();

  @Override
  // 在每个 Mapper 启动时只运行一次
  protected void setup(Context context) throws IOException, InterruptedException {
    String taskId = context.getTaskAttemptID().toString();
    // 第一个 Mapper 才输出表头
    if (taskId.contains("_m_000000")) {
      context.write(new Text("S#\tC#\tScore"), NullWritable.get());
    }

    // 从分布式缓存中加载 Course 表
    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles != null) {
      for (URI uri : cacheFiles) {
        Path path = new Path(uri.getPath());
        try (BufferedReader reader = new BufferedReader(new FileReader(path.getName()))) {
          String line;
          while ((line = reader.readLine()) != null) {
            if (line.startsWith("C#"))
              continue; // 跳过表头
            String[] parts = line.split("\t");
            if (parts.length >= 1) {
              // System.out.println("total: " + parts[0]);
              courseSet.add(parts[0]); // 添加课程号 C#
            }
          }
        }
      }
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // System.out.println("KEY: " + key.toString());
    // System.out.println("VALUE: " + value.toString());

    String line = value.toString();
    if (line.startsWith("S#") || line.startsWith("C#") || line.isEmpty())
      return;

    String[] parts = line.split("\t");
    if (parts.length < 3)
      return;

    // 提取课程号
    String courseID = parts[1];
    if (courseSet.contains(courseID)) {
      context.write(new Text(line), NullWritable.get());
    }
  }
}

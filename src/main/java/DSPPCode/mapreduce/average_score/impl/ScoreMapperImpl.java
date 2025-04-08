package DSPPCode.mapreduce.average_score.impl;

import DSPPCode.mapreduce.average_score.question.ScoreMapper;
import DSPPCode.mapreduce.average_score.question.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ScoreMapperImpl extends ScoreMapper {

  private IntWritable score = new IntWritable();
  private Text course = new Text();

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException{
    String[] tokens = value.toString().split(",");

    for(int i = 0; i < 3; i++){
      score.set(Integer.parseInt(tokens[i + 1]));
      course.set(Util.getCourseName(i));
      context.write(course, score);
    }

  }
}

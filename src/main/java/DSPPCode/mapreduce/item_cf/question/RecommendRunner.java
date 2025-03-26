package DSPPCode.mapreduce.item_cf.question;

import DSPPCode.mapreduce.item_cf.impl.RecommendMapperImpl;
import DSPPCode.mapreduce.item_cf.impl.RecommendReducerImpl;
import DSPPCode.mapreduce.item_cf.question.utils.Constants;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class RecommendRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    getConf().set(Constants.USER_SCORE_MATRIX_FILE_NAME, args[0]);
    getConf().set(Constants.CO_OCCURRENCE_MATRIX_FILE_NAME, args[1]);
    getConf().setInt(Constants.USER_COUNT, Integer.parseInt(args[2]));
    getConf().setInt(Constants.ITEM_COUNT, Integer.parseInt(args[3]));

    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[4]));
    FileInputFormat.addInputPath(job, new Path(args[5]));
    FileOutputFormat.setOutputPath(job, new Path(args[6]));

    job.setMapperClass(RecommendMapperImpl.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(RecommendReducerImpl.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

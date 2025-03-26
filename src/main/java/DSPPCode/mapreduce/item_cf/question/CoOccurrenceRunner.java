package DSPPCode.mapreduce.item_cf.question;

import DSPPCode.mapreduce.item_cf.impl.CoOccurrenceMapperImpl;
import DSPPCode.mapreduce.item_cf.impl.CoOccurrenceReducerImpl;
import DSPPCode.mapreduce.item_cf.question.utils.Constants;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class CoOccurrenceRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    getConf().setInt(Constants.USER_COUNT, Integer.parseInt(args[0]));
    getConf().setInt(Constants.ITEM_COUNT, Integer.parseInt(args[1]));

    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    job.setMapperClass(CoOccurrenceMapperImpl.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(CoOccurrenceReducerImpl.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

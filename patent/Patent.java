import java.io.IOException;
import java.util.StringTokenizer;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Patent {    
    public static class TaskMapper
        extends Mapper<Object, Text, Text, Text>{
            Logger log = Logger.getLogger(TaskMapper.class.getName());
            private Text kk = new Text();
            private Text vv = new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // log.info( "Mapper's phase: " );

                StringTokenizer itr = new StringTokenizer(value.toString(), ".");

                while (itr.hasMoreTokens()) {
                    kk.set(itr.nextToken());
                    vv.set(itr.nextToken());
                    context.write(kk, vv);
                }


            }
    }


    public static class TaskReducer
        extends Reducer<Text, Text, Text, IntWritable> {
            Logger log = Logger.getLogger(TaskReducer.class.getName());
            IntWritable res = new IntWritable();
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Output types of a combiner must match output types of a mapper. 
            int sum = 0;
            for (Text val : values) {
                sum += 1;
            }
            res.set(sum); 
            context.write(key,  res);
            }
        }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "patent program");
    job.setJarByClass(Patent.class);
    job.setMapperClass(TaskMapper.class);
    // job.setCombinerClass(TaskReducer.class);
    job.setReducerClass(TaskReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

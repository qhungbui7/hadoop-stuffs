import java.io.IOException;
import java.util.StringTokenizer;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


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


public class MusicTrack {    
    public static class TaskMapper
        extends Mapper<Object, Text, Text, Text>{
            Logger log = Logger.getLogger(TaskMapper.class.getName());
            private Text UserId = new Text();
            private Text TrackId = new Text();
            private Text Shared = new Text();
            private Text Radio = new Text();
            private Text Skip = new Text();

            // private IntWritable one = new IntWritable(1); 
            private Text one = new Text("1"); 
            private Text zero = new Text("0"); 

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // log.info( "Mapper's phase: " );

                StringTokenizer itr = new StringTokenizer(value.toString(), "|");

                while (itr.hasMoreTokens()) {
                    String user_id = new String(itr.nextToken());
                    UserId.set(user_id); // count number of unique listeners

                    String track_id = new String(itr.nextToken());
                    TrackId.set(track_id);

                    String shared = new String(itr.nextToken());
                    Shared.set(shared);

                    String radio = new String(itr.nextToken());
                    Radio.set(radio);

                    String skip = new String(itr.nextToken());
                    Skip.set(skip);


                    context.write(new Text("UniqueUserId"), UserId);
                    context.write(new Text("TotalTrackId " + TrackId.toString()), one);


                    
                    if (Integer.parseInt(shared) ==  1){
                        context.write(new Text("SharedTrackId_" + TrackId.toString()), one);
                    } else if (Integer.parseInt(shared) ==  0){
                        context.write(new Text("SharedTrackId_" + TrackId.toString()), zero);
                    }

                    if (Integer.parseInt(skip) == 1 && Integer.parseInt(radio) == 1){
                        context.write(new Text("SkipRadioTrackId_" + TrackId.toString()), one);
                        context.write(new Text("RadioTrackId_" + TrackId.toString()), zero);

                    } else if (Integer.parseInt(skip) == 0 && Integer.parseInt(radio) == 1){
                        context.write(new Text("SkipRadioTrackId_" + TrackId.toString()), zero);
                        context.write(new Text("RadioTrackId_" + TrackId.toString()), one);
                    }


                    
                }


            }
    }

    // public static class TaskCombiner
    //     extends Reducer<Text, Text, Text, IntWritable> {
    //         Logger log = Logger.getLogger(TaskCombiner.class.getName());

    //         public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    //         int sum = 0;
    //         for (Text val : values) {
    //             sum += 1;
    //         }
    //         context.write(key,  new IntWritable(sum));
    //         }
    //     }

    public static class TaskReducer
        extends Reducer<Text, Text, Text, IntWritable> {
            Logger log = Logger.getLogger(TaskReducer.class.getName());
            IntWritable res = new IntWritable();
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Output types of a combiner must match output types of a mapper.
            Text label = new Text("UniqueUserId"); 
            // log.info(key.toString() + " " + label.toString()); 
            // log.info("" +(key.toString() == label.toString())); 

            if (key.toString().equals( label.toString())){
                Set<Text> userIdSet = new HashSet<Text>();
                for (Text val : values) {
                    userIdSet.add(val); 
                    log.info(val.toString()); 

                }
                log.info(userIdSet.size() + "" ); 
                context.write(new Text("NumUniqueUserID"),  new IntWritable(userIdSet.size()));

            } else {
                int sum = 0;
                for (Text val : values) {
                    if (Integer.parseInt(val.toString()) == 1)
                        sum += 1;
                }
                res.set(sum); 
                context.write(key,  res);
                }
            }
        }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "music track program");
    job.setJarByClass(MusicTrack.class);
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

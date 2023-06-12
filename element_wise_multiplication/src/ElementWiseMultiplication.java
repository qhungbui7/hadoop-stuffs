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


public class ElementWiseMultiplication {    
    public static class TaskMapper
        extends Mapper<Object, Text, Text, Text>{
            Logger log = Logger.getLogger(TaskMapper.class.getName());
            private Text kk = new Text();
            private Text vv = new Text();
            private Text word = new Text();
            int i = 0;
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

                StringTokenizer itr = new StringTokenizer(value.toString());
                int j = 0 ; 
                
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    kk.set(new Text(Integer.toString(j))); 
                    vv.set(new Text(Integer.toString(i) + ";" + word.toString()));
                    context.write(kk, vv);
                    j += 1;
                    // log.info(Integer.toString(j));
                    // log.info(Integer.toString(i) + ";" + itr.nextToken().toString());
                }

            i += 1;


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

    public static class TaskReducer // can optmize the bandwidth by removing duplicates in the combiner stage
        extends Reducer<Text, Text, Text, Text> {
            Logger log = Logger.getLogger(TaskReducer.class.getName());

            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Output types of a combiner must match output types of a mapper. 
            HashMap<String, Integer> reduceSumRow = new HashMap<String, Integer>();
            for (Text val : values) {
                String[] token = val.toString().split(";"); 
                if (reduceSumRow.containsKey(token[0])){
                    reduceSumRow.put(token[0], reduceSumRow.get(token[0]) * Integer.parseInt(token[1]));
                }
                else{
                    reduceSumRow.put(token[0], Integer.parseInt(token[1]));
                }
            }

            String res = "";
            
            for (Map.Entry<String, Integer> entry : reduceSumRow.entrySet()) {
                String kk = entry.getKey();
                int vv = entry.getValue();
                res += " " ;
                res += Integer.toString(vv); 
            }  

            context.write(key, new Text(res));
            }
        }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "element-wise program");
    job.setJarByClass(ElementWiseMultiplication.class);
    job.setMapperClass(TaskMapper.class);
    // job.setCombinerClass(TaskReducer.class);
    job.setReducerClass(TaskReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

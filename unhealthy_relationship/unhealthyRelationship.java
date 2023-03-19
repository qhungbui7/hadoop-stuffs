import java.io.IOException;
import java.util.StringTokenizer;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unhealthy_relationship {

  public static class TaskMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable negone = new IntWritable(-1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

	// Map<String, Integer> dictionary = new HashMap<String, Integer>();

	int sentinel = 0;

	while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	  String target = word.toString();
	  /* if (dictionary.containsKey(target) == false){
	  	dictionary.put(target, 0);
	  } */
	  
	  if (sentinel % 2 == 0) {
	  	// dictionary.put(target, dictionary.get(target) + 1);
		context.write(word,  one);	  
	  } else {
        	// dictionary.put(target, dictionary.get(target) - 1);
        	context.write(word,  negone);
	  }
	  
	  
        sentinel += 1;
	 
      }
	/* for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
  		String kk = entry.getKey();
  		int vv = entry.getValue();
  		context.write(new Text(kk),  new IntWritable(vv));
	} */

    }
  }
  public static class TaskCombiner
       extends Reducer<Text, IntWritable, Text, IntWritable>{
    // Combiners is mini- Reducer . They follow the same signature of Reducer itself
    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();
    private IntWritable result = new IntWritable();

    public void combine(Text key, Iterable<IntWritable> values, Context context
                    ) throws IOException, InterruptedException {
      /* StringTokenizer itr = new StringTokenizer(value.toString());

	Map<String, Integer> dictionary = new HashMap<String, Integer>();

	int sentinel = 0;

	while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	  String target = word.toString();
	  if (dictionary.containsKey(target) == false){
	  	dictionary.put(target, 0);
	  } 
	  
	  if (sentinel % 2 == 0) {
	  	dictionary.put(target, dictionary.get(target) + 1);
	  } else {
        	dictionary.put(target, dictionary.get(target) - 1);
        }
	  
	  
        sentinel += 1;
      }
	for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
  		String kk = entry.getKey();
  		int vv = entry.getValue();
  		context.write(new Text(kk),  new IntWritable(vv));
	} */ 
     int sum = 0;
     for (IntWritable val : values) {
       sum += val.get();
     }
     result.set(sum);
     context.write(key, result);

    }
  }
  public static class TaskReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    // private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
	String status = "";
	if (sum > 0){
		status = "pos";
	} else if (sum == 0){
		status = "eq";
	}
	else {
		status = "neg";
	}

	
      context.write(key, new Text(status));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "unhealthy relationship");
    job.setJarByClass(Unhealthy_relationship.class);
    job.setMapperClass(TaskMapper.class);
    job.setCombinerClass(TaskCombiner.class);
    job.setReducerClass(TaskReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

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

public class CountConnectedComponents {    
    public static class TaskMapper
        extends Mapper<Object, Text, IntWritable, IntWritable>{

            private Text word = new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
                int sentinel = 0;

                word.set(itr.nextToken());
                int vertex = Integer.parseInt(word.toString());

                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    int adjacentVertex = Integer.parseInt(word.toString());

                    int kk = Math.min(vertex, adjacentVertex);
                    int vv = Math.max(vertex, adjacentVertex);
                    
                    context.write(new IntWritable(kk), new IntWritable(vv));
                    sentinel += 1; 
                }
                if (sentinel == 0){ // no adjacent
                    context.write(new IntWritable(vertex), new IntWritable(-1));
                }

            }
    }
    // public static class TaskCombiner
    // extends Reducer<Text, Text, Text, IntWritable>{
    //     private IntWritable result = new IntWritable();
    //     // edge will have the smaller number as key, remove duplicate edge, return a list of adjacent vertex
    //     public void combine(Text key, Iterable<IntWritable> values, Context context
    //                     ) throws IOException, InterruptedException {

    //         int sum = 0;
    //         for (IntWritable val : values) {
    //             sum += val.get();
    //         }
    //         result.set(sum);
    //         context.write(key, result);

    //     }
    // }
    public static class DisjointSetUnion {
        private  Map<Integer, Integer> fathers = new HashMap<Integer, Integer>();
        
        public int getFather(int idx){
            int fat = fathers.get(idx);
            if (fat < 0)
                return idx; 
            else
                return getFather(fat);
        }
        public void merge(int left_idx, int right_idx){
            initialize(right_idx); 

            int top_left_idx = getFather(left_idx);
            int top_right_idx = getFather(right_idx);
            if (top_left_idx == top_left_idx){
                return; 
            }
            int num_left = getSize(top_left_idx); 
            int num_right = getSize(top_right_idx); 

            if (num_left > num_right){
                fathers.put(top_left_idx, num_left + num_right); 
                fathers.put(top_right_idx, top_left_idx);
            }
            else {
                fathers.put(top_right_idx, num_left + num_right); 
                fathers.put(top_left_idx, top_right_idx);
            }
            return; 
        }
        public int getSize(int highestNode){
            return -fathers.get(highestNode);
        }
        public void initialize(int idx){
            if (fathers.containsKey(idx) == false){ 
                fathers.put(idx, -1);
            }
            return; 
        }
        public Map<Integer, Integer> getInside(){
            return fathers; 
        }
        

    }

    public static class TaskReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
            
            DisjointSetUnion dsu = new DisjointSetUnion(); 
            public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                // received edge information
                // if both vertices are in the same region: do nothing
                // if both vertices are in the different region: update the smaller region
                // if 1/2 verices in a region: make the unemployed vertices be in that region
                // if vertices are not in any region: create a new region from these verices

                // an array for count
                // an array for the primal node
                
                // shared memory: K, V: K: <node, the identity of the graph>  

                // to parallel them, 
                // mapper become return edge   
                
                // combiner building the graph from edges of the local files, return key for vertex, value for its edge, called it a sub-graph
                // Input: <K, V>: K: node, V: connected node 
                // In this step, we build a adjacency list of each vertex, then mark the sub-graph (adjacency list) by a identity (every node's graph identity will be reassign) . the later sub-graph (adjacency list) must follow the sooner identity
                    // build the components as normal
                // optimize at the combiner phase, reduce the number of edge vastly
                // return <K, V>, K: node, V - connected graph,  identity of its sub-graph (adjacency list)

                // reducer do the same to the combiner, but the range by now is multiple files 
                // consider each sub-graph as the connected node
                // connected these graph with the same K
                // return <K, V> K: node, V: identity of its graph

                // reducer become merging the graph, by consider each pair node of graph  ???, key for the identity of each graph, if they have the same node, means they are merge-able 
                // output of reducer phase become return id of graph and each vertex inside



                int vertex = key.get();
                dsu.initialize(vertex); 
                for (IntWritable adjacentVertex: values){
                    int adjVertex = adjacentVertex.get(); 
                    if (adjVertex == -1){
                        continue;  
                    }
                    dsu.merge(vertex, adjVertex); 
                }
                for (Map.Entry<Integer, Integer> entry : dsu.getInside().entrySet()) {
  		            int kk = entry.getKey();
  		            int vv = entry.getValue();
  		            context.write(new IntWritable(kk),  new IntWritable(vv));
  		            context.write(new IntWritable(999999), new IntWritable(999999) );

	            }    

            }
        }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "count number of connected components program");
    job.setJarByClass(CountConnectedComponents.class);
    job.setMapperClass(TaskMapper.class);
    // job.setCombinerClass(TaskCombiner.class);
    job.setCombinerClass(TaskReducer.class);
    job.setReducerClass(TaskReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

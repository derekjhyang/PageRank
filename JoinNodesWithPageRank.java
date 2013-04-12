package ParallelPageRank;

import java.io.IOException;
import java.util.Map;

import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class JoinNodesWithPageRank extends AbstractJob {
  
  private Path nodesIndexPath;
  private Path pageRankPath;
  private Path output;
  
  public static final String PAGERANK_PATH_PARAM = "pagerank.path";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new JoinNodesWithPageRank(), args);  
  } // main()
  
  private void prepareArguments(String[] args) throws IOException {
    // set arguments template.
    addOption("nodesIndexFile", 
      	  "n", 
    		  "The nodes index sequence file", 
    		  true);
    
    addOption("pageRankFile", 
    		  "p",
    		  "The pagerank sequence vector file",
    		  true);
    addOutputOption();
    
    // read actual arguments and then set attributes.
    Map<String,String> arguments = super.parseArguments(args);
    nodesIndexPath = new Path(arguments.get("--nodesIndexFile"));
    pageRankPath   = new Path(arguments.get("--pageRankFile"));
    output         = new Path(arguments.get("--output"));
    
    // 根據conf來取得作業的檔案系統並且刪除output.
    output.getFileSystem(getConf()).delete(output, true);
  
  } // prepareArguments()
  
  private void join() throws IOException,
                             InterruptedException,
                             ClassNotFoundException {
    
   Job job = prepareJob(nodesIndexPath,
	                       output,
                           SequenceFileInputFormat.class,
                           JoinMapper.class,
                           NullWritable.class,
                           Text.class,
                           Reducer.class,
                           NullWritable.class,
                           Text.class,
                           TextOutputFormat.class); ///////////////////////////////////////////////////
    
	job.getConfiguration().set(PAGERANK_PATH_PARAM, pageRankPath.toString());
    
	job.setJarByClass(JoinNodesWithPageRank.class);
    
    job.waitForCompletion(true);
    
  } // join()
  
  public static class JoinMapper extends
      Mapper<IntWritable, Text, NullWritable, Text> {
    
    private Vector pageRank;
    
    protected void setup(Context context) throws IOException,
                                                 InterruptedException {
      super.setup(context);
      
      // read pagerank vector.
      Path pageRankPath = 
          new Path(context.getConfiguration().get(PAGERANK_PATH_PARAM));
      
      pageRank = VectorWritable.readVector(pageRankPath.getFileSystem(context.getConfiguration()).open(pageRankPath));
    
    } // setup()
    
    protected void map(IntWritable key,
    		           Text value,
    		           Context context) throws IOException, 
    		                                   InterruptedException {
    	context.write(NullWritable.get(), 
    			      new Text(value + "\t" + pageRank.get(key.get())));
    } // map()
  } // class JoinMapper
  
  public int run(String[] args) throws Exception {
    // prepare arguments.
    prepareArguments(args);
    
	// join.
    join();
    
    return 0;
  } // run()

} // public class JoinNodesWithPageRank

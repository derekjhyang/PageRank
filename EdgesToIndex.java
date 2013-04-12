package ParallelPageRank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job; // hadoop.mapreduce for later version.
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 *  幫每一個有向邊用(nodeIndex, nodeIndex)的方式作indexing.
 *  EX: 0:PageA ==> A網頁的index=0
 *      1:PageB ==> B網頁的index=1
 *      若PageA有link連到PageB 則原本的link
 *      ( PageA, PageB )改成 ( 0, 1 )
 *  
 * @author hswayne
 *
 */
// AbstractJob 為大部分Mahout Job的Super Class
public class EdgesToIndex extends AbstractJob {
  
  private Path inputPath;
  private Path outputPath;
  private Path indexPath;
  
  private int numEdges;
  
  private static final String INDEX_PATH_PARAM = "index.path";
  
  
  public EdgesToIndex() {
    super();
    setConf(new Configuration());
  } // EdgesToIndex()
  
  public int getNumEdges() {
  	return numEdges;
  } // getNumEdges()
  
  private void prepareArguments(String[] args) throws IOException {
    // set input/output option.
    addInputOption();
    addOutputOption();
		
    // add index-path option.
    addOption("indexPath", 
			  "ip", 
              "File which contains the index of the nodes of the graph", 
              true);
		
    // read the actual arguments and set their corresponding attributes.
    Map<String, String> arguments = super.parseArguments(args);
    inputPath  = new Path(arguments.get("--input"));
    outputPath = new Path(arguments.get("--output"));
    indexPath  = new Path(arguments.get("--indexPath"));
  } // prepareArguments() 
	
  /*
   * Reads the input file and assigns each line an index.
   */
  private void createIndex() throws IOException, 
                                    InterruptedException,
                                    ClassNotFoundException {
	// job preparation.
	Job job = prepareJob(inputPath,                     // job input file path. ( read edges-file )
			             outputPath,                    // job output file path. ( output edge-index file )
	    		         TextInputFormat.class,         // the format of input file. (LongWritable/Text)
	    		         EdgesToIndexMapper.class,      // mapper.
	    		         IntWritable.class,             // mapper output key
	    		         IntWritable.class,             // mapper output value
	    		         Reducer.class,
	    		         IntWritable.class,
	    		         IntWritable.class,
	    		         SequenceFileOutputFormat.class);
    
    // set job configuration.       name            value
    job.getConfiguration().set(INDEX_PATH_PARAM, indexPath.toString()); // setting node-index file.
    
    job.waitForCompletion(true);
    
    // get number of edges.
    numEdges = (int) job.getCounters()
    		             .findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS")
    		             .getValue();
    
  } // createIndex()

  private static class EdgesToIndexMapper extends 
     Mapper<Object, Text, IntWritable, IntWritable> {   // 以Object當Mapper的Input Key是表示此key沒有用到故所以忽略它
	  
    private static final Pattern SEPARATOR = Pattern.compile("\t");
    private Map<Text, IntWritable> index = new HashMap<Text, IntWritable>();
	
    // call only once before start the map task.
    protected void setup(Context context) throws IOException,
                                                 InterruptedException {
      // setup.
      super.setup(context);
      
      // read index files. index-files本身為sequence files.
      //
      // SequenceFileDirIterator(org.apache.hadoop.fs.Path path, 
      //                         PathType pathType, 
      //                         org.apache.hadoop.fs.PathFilter filter, 
      //                         Comparator<org.apache.hadoop.fs.FileStatus> ordering, 
      //                         boolean reuseKeyValueInstances, 
      //                         org.apache.hadoop.conf.Configuration conf) 
      //    Constructor that uses either FileSystem.listStatus(Path) or FileSystem.globStatus(Path) 
      //    to obtain list of files to iterate over (depending on pathType parameter).
      // 
      SequenceFileDirIterator<IntWritable, Text> iterator =
    		  new SequenceFileDirIterator<IntWritable, Text>(                          // Path(Path Parent, Path child)
    				  new Path(context.getConfiguration().get(INDEX_PATH_PARAM), "*"), // node-index file.
    				  PathType.GLOB, 
    				  null,
    				  null,
    				  false,
    				  context.getConfiguration());
      
      /*
       *  建立起 <網址URL,Index> 形式的索引清單 不過這裡的data是以nodeID代替URL.
       */
      // create index maps.
      while (iterator.hasNext()) {
        Pair<IntWritable, Text> pair = iterator.next();
        
        // inverse it. ( Text / IntWritable )
        index.put(pair.getSecond(), pair.getFirst());
      
      } // while
      
    } // setup()
    
    protected void map(Object key,
                       Text value,
                       Context context) throws IOException,
                                               InterruptedException {
      String[] tokens = SEPARATOR.split(value.toString());
      if (tokens.length != 2) {
	    throw new IllegalArgumentException("Edge file format has <node>\t<node> per line");	
      } // if
		
	  Text startNode = new Text(tokens[0]); // start Vertex
	  Text endNode   = new Text(tokens[1]); // end Vertex
		
	  //if ( startNode.toString().equals(endNode.toString())) {
	  //  return;
	  //} // if
		
	  if ( !index.containsKey(startNode) || 
           !index.containsKey(endNode)) {
	    throw new IllegalArgumentException("Unknown node name");
	  } // if
	
	  //                 key                 value  
	  context.write(index.get(startNode), index.get(endNode));
    
    } // map()
  } // class EdgesToIndexMapper
  
  public int run(String[] args) throws Exception {
    // prepare arguments.
    prepareArguments(args);
		  
    // create index.
    createIndex();
			
    return 0;
  } // run()

} // public class EdgesToIndex

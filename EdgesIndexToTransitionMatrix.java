package ParallelPageRank;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


/**
 * creates the transition matrix.
 * 
 */
public class EdgesIndexToTransitionMatrix extends AbstractJob {
  
  private Path inputPath; // input file path.
  private Path outputPath; // output file path.
  private Path degreesPath; // degree file path including degrees for each node.

  private int numNodes;
  
  // path access key.
  private static final String DEGREES_FILE = "degrees";
  private static final String DEGREE_PATH_PARAM = "degree.path";
  private static final String NUMBER_NODES_PARAM = "numNodes";
	
  
  public EdgesIndexToTransitionMatrix() {
    super();
    setConf(new Configuration());
  } // EdgesIndexToTransitionMatrix()
	
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new EdgesIndexToTransitionMatrix(), args);
  } // main()
	
  /**
   * sets and reads the running arguments for the calculation
   */
  private void prepareArguments(String[] args) throws IOException {
	// input option.
	addInputOption();
	// output option.
	addOutputOption();
	// number of nodes option.
	addOption("numNodes",
			  "nn",
			  "Number of nodes in the graph",
			  true);
	// read the actual arguments and set their corresponding attributes.
	Map<String,String> arguments = super.parseArguments(args);
	inputPath   = new Path(arguments.get("--input"));
	outputPath  = new Path(arguments.get("--output"));
	degreesPath = new Path(arguments.get("--tempDir"),
			               DEGREES_FILE + "-" + ParallelPageRankJob.getCurrentTimeStamp());
	numNodes = Integer.parseInt(arguments.get("--numNodes"));
  } // prepareArguments()
  
  /**
   * Counts the out-degree of each node in the graph.
   */
  private void countDegrees() throws IOException,
                                     InterruptedException,
                                     ClassNotFoundException {
    // prepare job.
    Job job = prepareJob(inputPath,                      // the input path of the input file
    		             degreesPath,                    // the output path of the output file
    		             SequenceFileInputFormat.class,  // the input format
    		             CountDegreeMapper.class,
    		             IntWritable.class,
    		             IntWritable.class,
    		             IntSumReducer.class,
    		             IntWritable.class,
    		             IntWritable.class,
    		             SequenceFileOutputFormat.class);
    job.setJarByClass(EdgesIndexToTransitionMatrix.class); //////////////////////////////////////////
    // set combiner.
    job.setCombinerClass(IntSumReducer.class);
    job.waitForCompletion(true);
  } // countDegrees()


  /**
    * Converts the graph in the transition matrix. Needs the edges and the
    * degrees of the nodes.
    * 
    */
  protected void createTransitionMatrix() throws IOException,
                                                 InterruptedException,
                                                 ClassNotFoundException {
    Job job = prepareJob(inputPath, // edge-index.
    		             outputPath,
    		             SequenceFileInputFormat.class,
    		             InverseMapper.class, // a mapper that swaps key and value.
    		             IntWritable.class,   // mapper output key
    		             IntWritable.class,   // mapper output value.
    		             CreateMatrixReducer.class, // reducer
    		             IntWritable.class,         // reducer output key
    		             VectorWritable.class,      // reducer output value
    		             SequenceFileOutputFormat.class);
    // set parameter.
    job.getConfiguration().set(DEGREE_PATH_PARAM, degreesPath.toString()); // degree file path setting.
    job.getConfiguration().set(NUMBER_NODES_PARAM, "" + numNodes);         // number of nodes setting.
    job.setJarByClass(EdgesIndexToTransitionMatrix.class);
    job.waitForCompletion(true);
  } // createTransitionMatrix()
  
  // CountDegreeMapper.
  static class CountDegreeMapper 
      extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

    /**
	  * Mapper for the node degree counter. //Needs edge file. 
	  * Emits each edge with start node as key and 1 as value.
	  * 
	  */
    protected void map(IntWritable key,
		               IntWritable value,
		               Context context) throws IOException, 
		                                       InterruptedException {
      context.write(key, new IntWritable(1));
    } // map()
    
  } // class CountDegreeMapper
  
  /**
   * Reducer for the transition matrix creation. Loads a vector with degree
   * weight for each node in memory
   */
  static class CreateMatrixReducer extends
      Reducer<IntWritable, IntWritable, IntWritable, VectorWritable> {
    private Vector weights;
    private int numNodes; 
  
    protected void setup(Context context) throws IOException,
                                                 InterruptedException {
      // default setup.
      super.setup(context);
      // number of vertex.
      numNodes = Integer.parseInt(context.getConfiguration().get(NUMBER_NODES_PARAM));
      // weights vector.
      weights = new DenseVector(numNodes);
      // read degree files.
      SequenceFileDirIterator<IntWritable, IntWritable> iterator =
          new SequenceFileDirIterator<IntWritable, IntWritable>(   // Constructor: Path(Path parent, Path child)
              new Path(context.getConfiguration().get(DEGREE_PATH_PARAM), "part*"), // 
              PathType.GLOB, // the set including files and dirs.
              null,
              null,
              false,
              context.getConfiguration());
      
      while (iterator.hasNext()) {
        Pair<IntWritable, IntWritable> pair = iterator.next();
        weights.set(pair.getFirst().get(), (1.0 / pair.getSecond().get()));
      } // while
    } // setup()
  
    protected void reduce(IntWritable key,
		                  Iterable<IntWritable> values,
		                  Context context) throws IOException,
                                                  InterruptedException {
      // create the row vector for the node.
      Vector vector = new RandomAccessSparseVector(numNodes);
      
      // set weights in vector.
      for (IntWritable startNode : values) {
        vector.set(startNode.get(), weights.get(startNode.get())); // ( nodeIndex, node-weight )
      } // for
      
      // output vector.
      context.write(key, new VectorWritable(vector));
    
    } // reducer()
    
  } // class CreateMatrixReducer
  
  public int run(String[] args) throws Exception {
	// prepare arguments and make it ready.
	prepareArguments(args);
		
	// count the degree for each node.
	countDegrees();
	
	
	// create the transition matrix.
	createTransitionMatrix();  
		  
    return 0;
  } // run()
} // public class EdgesIndexToTransitionMatrix

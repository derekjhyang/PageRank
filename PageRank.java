package ParallelPageRank;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

import com.google.common.io.Closeables;

/* computes pageRank from the transition matrix.
 * 
 * Class AbstractJob
 *     java.lang.Object
 *       |-org.apache.hadoop.conf.Configured
 *           |-org.apache.mahout.common.AbstractJob
 * All Implemented Interfaces:
 *     org.apache.hadoop.conf.Configurable, org.apache.hadoop.util.Tool
 */
public class PageRank extends AbstractJob {
  
  private double damplingFactor;
  private int iterations;
  private int numNodes;
  
  private Path inputPath;
  private Path outputPath;
  private Path tempPath;
  
  private DistributedRowMatrix matrix;
  
  private Vector pageRank;
  private Vector damplingVector;
  
  private static final String MATRIX_TEMP_FILE = "temp_trainstion_matrix";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new PageRank(), args);  
  } // main()
  
  public PageRank() {
    super();
    setConf(new Configuration());
  } // PageRank()
  
  public int run(String[] args) throws Exception {
    
    // prepare the arguments.
  	prepareArguments(args);
    
    // load transition matrix and 
  	// initialize pagerank and dampling-factor vector.
    loadMatrixAndVectors();
    
    // calculate pagerank iteratively.
    for ( int i = 0; i < iterations; i++ )
      calculatePageRank();
    
    // save pagerank.
    outputPageRank();
    
    return 0;
  } // run()

  /*
   *  sets and reads the argument 
   */
  private void prepareArguments(String[] args) throws IOException {
    
    addInputOption();
    addOutputOption();
    
    addOption("numNodes", "nn", "Number of nodes in the graph", true);
    addOption("iterations", "it", "Number of iterations", "1");
    addOption("damplingFactor", "df", "The dampling-factor", "0.8" );
    
    // read arguments and set attributes.
    Map<String, String> arguments = super.parseArguments(args);
    inputPath  = new Path(arguments.get("--input"));
    outputPath = new Path(arguments.get("--output"));
    tempPath   = new Path(arguments.get("--tempDir"),
    		                  MATRIX_TEMP_FILE + "-" + System.currentTimeMillis());
    numNodes   = Integer.parseInt(arguments.get("--numNodes"));
    iterations = Integer.parseInt(arguments.get("--iterations"));
    damplingFactor = Double.parseDouble(arguments.get("--damplingFactor"));
    
  } // prepareArguments()
  
  /**
   *  set the transition matrix and the initial pagerank and damplink vector.
   */
  private void loadMatrixAndVectors() {
    
  	/**
  	 *  load matrix.
  	 *  
  	 *  DistributedRowMatrix(org.apache.hadoop.fs.Path inputPath, 
  	 *                       org.apache.hadoop.fs.Path outputTmpPath, 
  	 *                       int numRows, 
  	 *                       int numCols) 
  	 */
  	matrix = new DistributedRowMatrix(inputPath,
  	 	                              tempPath,
  		                              numNodes,
  		                              numNodes);
  	matrix.setConf(getConf());
  	
  	
  	/**
  	 *  load vectors.
  	 */
  	pageRank       = new DenseVector(numNodes).assign(1.0 / numNodes);
  	damplingVector = new DenseVector(numNodes)
  	                 .assign((1.0 - damplingFactor) / numNodes);
  	
  	
  } // loadMatrixAndVectors()

  /**
   *  calculates the page rank for each iteration.
   */
  private void calculatePageRank() {
  	Vector temp = matrix.times(pageRank);  // Mv, where M: transition matrix, v: page-rank vector.
  	temp = temp.times(damplingFactor);     // B: damplingFactor
  	pageRank = temp.plus(damplingVector);  // (1-B)e/n
  } // calculatePageRank()
  
  
  /**
   *  saves the page rank vector in the output file.
   */
  private void outputPageRank() throws IOException {
  	FSDataOutputStream outputStream = 
  			outputPath.getFileSystem(new Configuration()).create(outputPath, true);
  	try {
      //                         DataOutput     Vector
  	  VectorWritable.writeVector(outputStream, pageRank);
  	} finally {
  	  Closeables.closeQuietly(outputStream);
  	}
  } // outputPageRank()
} // public class PageRank

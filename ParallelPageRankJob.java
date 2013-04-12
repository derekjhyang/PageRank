package ParallelPageRank;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;



public class ParallelPageRankJob extends AbstractJob {
  
  private String nodes;
  private String edges;
  private String output;
  private static Long timeStamp;
  
  private int iterations;
  private double damplingFactor; // 阻尼係數
  
  private Path nodesIndexPath;
  private Path edgesIndexPath;
  private Path degreesPath;
  private Path transitionMatrixPath;
  private Path pageRankPath;
  
  public static long getCurrentTimeStamp() {
    return timeStamp;
  } // 
  
  public static void main(String[] args) throws Exception {
  ToolRunner.run(new Configuration(), new ParallelPageRankJob(), args);  
  } // main()
  
  public int run(String[] args) throws Exception {
	// prepare arguments
    prepareArguments(args);
    
    /*
     * create nodes index
     */
    String[] nodesToIndexArgs = {"-i", nodes, "-o", nodesIndexPath.toString()};
    LineIndexer nodesIndexer = new LineIndexer();
    ToolRunner.run(nodesIndexer, nodesToIndexArgs);
    int numNodes = nodesIndexer.getNumLines();
    
    /*
     * create edges index
     */
    String[] edgesToIndexArgs = {"-i", edges, 
    		                     "-o", edgesIndexPath.toString(), 
    		                     "-ip", nodesIndexPath.toString()};
    EdgesToIndex edgesIndexer = new EdgesToIndex();
    ToolRunner.run(edgesIndexer, edgesToIndexArgs);
    int numEdges = edgesIndexer.getNumEdges();

    /*
     * create transition matrix
     */
    String[] edgesIndexToTransitionMatrixArgs = {"-i",
                                                 edgesIndexPath.toString(), 
                                                 "-o",
                                                 transitionMatrixPath.toString(),
                                                 "--tempDir",
                                                 degreesPath.toString(),                                           
                                                 "-nn", "" + numNodes};
    //
    // run(Tool tool, String[] args) 
    //    Runs the Tool with its Configuration.
    //
    ToolRunner.run(new EdgesIndexToTransitionMatrix(), edgesIndexToTransitionMatrixArgs);
    
    
    /*
     * calculate pagerank
     */
    String[] pageRankArgs = {"-i", 
    		                 transitionMatrixPath.toString(), 
    		                 "-o",
                             pageRankPath.toString(), 
                             "-nn", 
                             "" + numNodes, "-it",
                             "" + iterations, 
                             "-df", "" + damplingFactor};
    ToolRunner.run(new PageRank(), pageRankArgs);
    
    
    /* 
     * join result to text file
     */
    String[] joinArgs = {"-n", 
    		             nodesIndexPath.toString(), 
    		             "-p",
                         pageRankPath.toString(), 
                         "-o", 
                         output};
    ToolRunner.run(new JoinNodesWithPageRank(), joinArgs);

    
    // output
    Path outputPath = new Path(output);
    //inputstream.
    FSDataInputStream stream = outputPath.getFileSystem(getConf()).open(
            new Path(outputPath, "part-r-00000"));
    
    LineReader reader = new LineReader(stream);
    Text line = new Text();
    double completeScore = 0.0;

    PriorityQueue<Pair<String, Double>> queue = 
    		new PriorityQueue<Pair<String, Double>>(numNodes, new PRComparator());
    while (reader.readLine(line) > 0) {
      String[] tokens = line.toString().split("\t");
      String node = tokens[0];                      // node id.
      Double pr   = Double.parseDouble(tokens[1]);  // pagerank value
      queue.add(new Pair<String, Double>(node, pr));
      completeScore += pr;
    } // while
    
    System.out.println("Nodes: " + numNodes);
    System.out.println("Edges: " + numEdges);
    System.out.println("Iterations: " + iterations);
    System.out.println("Complete PR-Score: " + completeScore);
    System.out.println("Pageranks in descending order:");
    
    while (!queue.isEmpty()) {
      Pair<String, Double> pair = queue.poll();
      System.out.println(pair.getFirst() + "\t" + pair.getSecond());
    } // while
   
    return 0;
  } // run()
  
  /**
   *  Page Rank Comparator: PRComparator
   *
   */
  private class PRComparator implements Comparator<Pair<String, Double>> {

	public int compare(Pair<String, Double> arg0, Pair<String, Double> arg1) {
	    return arg1.getSecond().compareTo(arg0.getSecond());
    } // compare()
	
  } // private class PRComparator
  
  public void prepareArguments(String[] args) throws IOException {
    // nodes-file option.
    addOption("nodesFile", "n", "The text file with the nodes in format <node>\\n", true);
    // edge-file option.
    addOption("edgesFile", "e", "The text file with the edges in format <startNode>\\t<endNode>\\n", true);
    // output option.
    addOutputOption();
    // iteration option.
    addOption("iterations", "it", "The number of iterations", true );
    // dampling factor.
    addOption("damplingFactor", "df", "The dampling factor of the random surface", "0.80" );
    
    // read argument and set the corresponding attributes.
    Map<String, String> arguments = super.parseArguments(args);
    nodes          = arguments.get("--nodesFile");
    edges          = arguments.get("--edgesFile");
    output         = arguments.get("--output");
    iterations     = Integer.parseInt(arguments.get("--iterations"));
    damplingFactor = Double.parseDouble(arguments.get("--damplingFactor"));
    
    Path temp = new Path(arguments.get("--tempDir"));
    degreesPath = temp;
    timeStamp = System.currentTimeMillis();
    nodesIndexPath       = new Path(temp, "nodesIndex-" + timeStamp);
    edgesIndexPath       = new Path(temp, "edgesIndex-" + timeStamp);
    transitionMatrixPath = new Path(temp, "transitionMatrix-" + timeStamp);
    pageRankPath         = new Path(temp, "pageRank-" + timeStamp);
  } // prepareArguments()
} // public class ParallelPageRankJob

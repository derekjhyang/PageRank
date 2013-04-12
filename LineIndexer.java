package ParallelPageRank;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.mahout.common.AbstractJob;

import com.google.common.io.Closeables;

/* [[assigns each line an index]]
 * 
 *  converts a text file to an index sequence file
 *  with \<index\>\t\<string\> per line.
 *  
 */
public class LineIndexer extends AbstractJob {
  
  private Path inputPath;
  private Path outputPath;
  private int numLines;
  
  public LineIndexer() {
    super();
    setConf(new Configuration());
  } // LineIndexer()

  public int getNumLines() {
    return numLines;
  } // getNumLines
  
  //--------------------------
  // set arguments template.
  //--------------------------
  private void prepareArguments(String[] args) throws IOException {    
	// input option.
    addInputOption();  // Add the default input directory option, 
		               // '-i' which takes a directory name as an argument.
    // output option.	
    addOutputOption(); // Add the default output directory option, 
		               // '-o' which takes a directory name as an argument.	
    
    Map<String,String> arguments = super.parseArguments(args);
    inputPath  = new Path(arguments.get("--input"));
    outputPath = new Path(arguments.get("--output"));
  } // prepareArguments()
  
  /*
   * Reads the input file and assigns each line an index.
   * 
   * 先根據input path讀取檔案然後，每讀取一行就以"<行數,內容>"格式寫入到sequence file. 
   */
  private void createIndex() throws IOException {
    // retrieve the input stream . 根據作業的環境設定檔取得所使用的FileSystem然後打開所指定路徑
    FSDataInputStream inputStream = 
	        inputPath.getFileSystem(getConf()).open(inputPath);
	  
    // Write key/value pairs to a sequence-format file
    Writer writer = SequenceFile.createWriter(outputPath.getFileSystem(getConf()), // file system
                                              getConf(),                           // configuration. 
                                              outputPath,                          // output path.
                                              IntWritable.class,                   // key
                                              Text.class);                         // value.
    try {
      // hadoop line reader.
      LineReader reader = new LineReader(inputStream);
      Text line  = new Text();
      int  index = 0;
      while (reader.readLine(line) > 0) {
	  	/* 
	  	 * void	append(Object key, Object val) 
         *    Append a key/value pair.
         * void	append(Writable key, Writable val) 
         *    Append a key/value pair.
         */    
        writer.append(new IntWritable(index++), line);
        
      } // while
	  	
      numLines = index; // set line number.
    
    } finally {
      Closeables.closeQuietly(inputStream);
      Closeables.closeQuietly(writer);
    } 
  } // createIndex()
  
  public int run(String[] args) throws Exception {
    // prepare arguments
    prepareArguments(args);
	  
    // create index.
    createIndex();
		
    return 0;
  } // run()
  
} // public class LineIndexer

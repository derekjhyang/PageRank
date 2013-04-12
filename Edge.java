package ParallelPageRank;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class Edge implements WritableComparable<Edge>, Cloneable {
  private Vertex start, end;
  
  public Edge() {} // Edge()
  
  public Edge(Vertex start, Vertex end) {
    this.start = start;
    this.end   = end;
  } // Edge()
  
  public Edge(long startVertexID, long endVertexID) {
    this(new Vertex(startVertexID), new Vertex(endVertexID));    
  } // Edge()
  
  // read its state from the binary data-streaming(DataInput).
  public void readFields(DataInput in) throws IOException {
    start = new Vertex();
    start.readFields(in);
    end = new Vertex();
    end.readFields(in);
  } // readFields()

  // write its state into the binary data-streaming(DataOutput).
  public void write(DataOutput out) throws IOException {
    start.write(out);
    end.write(out);
  } // write()

  public Vertex startVertex() {
    return start;  
  } // startVertex()
  
  public Vertex endVertex() {
	return end;  
  } // endVertex()
  
  public String toString() {
    return "(" + start.getID() + "," + end.getID() + ")";
  } // toString()
  
  public boolean equals(Object o) {
    if ( o instanceof Edge ) {
      Edge other = (Edge) o;
      return start.equals(other.start) && end.equals(other.end);
    } // if
    return false;
  } // equals()
  
  // via Google CompareChain.
  public int compareTo(Edge other) {
    return ComparisonChain.start()
    	   .compare( start, other.start)
    	   .compare( end, other.end).result();
  } // compareTo()

} // class Edge

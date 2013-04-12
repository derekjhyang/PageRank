package ParallelPageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;


/**
 *  package org.apache.hadoop.io;
 *  
 *  public interface WritableComparable<T> extends Writable, Comparable<T>{
 *  }
 *  
 */
public class Vertex implements Comparable<Vertex> {
  private LongWritable id;
  
  public Vertex() {} 
  
  public Vertex(long id) {
    this.id = new LongWritable(id);  
  } // Vertex()
  
  public long getID() {
    return id.get();
  } // getID()
  
  public String toString() {
    return id.toString();  
  } // getIDIntVal()
  
  public void setID(long id) {
    this.id = new LongWritable(id);
  } // setID()

  public void readFields(DataInput in) throws IOException {
    id = new LongWritable();
    id.readFields(in);
  } // readField()

  // write its state into binary data-streaming.
  public void write(DataOutput out) throws IOException {
    id.write(out);
  } // write()

  public int compareTo(Vertex v) {
    return (int)(this.id.get() - v.getID());
  } // compareTo()

} // public class Vertex

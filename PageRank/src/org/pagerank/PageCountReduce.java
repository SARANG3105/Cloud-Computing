/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


//This is the reducer for counting pages which receives the input from PageCountMap.
public class PageCountReduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable >  {
	 @Override 
     public void reduce( Text line,  Iterable<IntWritable > counts,  Context context)
        throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {					//the for loop just sums the occurrences of titles in the map			
            sum  += count.get();								//output and writes the sum as output 
         }														//we get the number of lines from this reducer.
        
         context.write(new Text("N"),  new IntWritable(sum));   //the output looks like [N	numberOfPages]
     }

}

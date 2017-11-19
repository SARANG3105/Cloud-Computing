/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//reducer for sorting takes input as pagerank,page
public class SortingReduce extends Reducer<  DoubleWritable,Text ,  Text ,  DoubleWritable >  {
	@Override 
	public void reduce( DoubleWritable rank,  Iterable<Text > counts,  Context context)
			throws IOException,  InterruptedException {   
		for(Text page:counts){
	//outputs the sorted page rank values in descending order
			//output is [page		rank]
			context.write(page,rank);
		}
}
}
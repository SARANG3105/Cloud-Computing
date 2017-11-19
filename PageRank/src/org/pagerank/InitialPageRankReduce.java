/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//reducer for InitialPageRankMap which assigns the initial page rank 1/N
public class InitialPageRankReduce extends Reducer<Text ,  Text ,  Text ,  Text >  {
	@Override 
	public void reduce( Text line,  Iterable<Text > counts,  Context context)
			throws IOException,  InterruptedException { 
		Double N= context.getConfiguration().getDouble("N",0.0);  //getting the configuration set in job for this MapReduce in driver.
																//The configuration returns the value of N retrieved using bufferedreader.
		
		Double initPageRank= 1/N;								//initial page rank for each page 
		String links="";					
		for ( Text count  : counts) {
			links=count.toString();
		}
		if(!line.equals("")){
			//The output is of the form [page	outlink1&&&&outlink2====initPageRank]
		
			context.write(new Text(line), new Text(links+"====="+String.valueOf(initPageRank)) );
	
	}
}
}
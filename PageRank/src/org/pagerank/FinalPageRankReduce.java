/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//reducer for calculating final page rank
public class FinalPageRankReduce extends Reducer<Text ,  Text ,  Text ,  Text >  {	
	@Override 
	public void reduce( Text line,  Iterable<Text > counts,  Context context)
			throws IOException,  InterruptedException {  
		String links="";double sum=0.0d;
//iterating over the counts 									
		for(Text count: counts){
			//split the value recieved from mapper at tab.
			
			String[] value= count.toString().split("\t");
//if array at index 0 has @: condition is true.			
			if(value[0].equals("@:")){
			
				if(value.length==2){
					links=value[1];
				}
			}else{
					if(value[0].equals("empt") && line.toString().equals("empt")){
						break;
					}else{
				//calculating the sum of values recieved by dividing the initial page rank with number of outlinks
						sum+=Double.parseDouble(value[0])/Double.parseDouble(value[1]);
						
					}
				}
			}
		
		
		if(!(links.equals(""))){
			//page rank formula to calculate final page rank.
			double pr= (0.85*sum)+0.15 ;
			context.write(new Text(line), new Text(links+"====="+pr));
		}
	}
}

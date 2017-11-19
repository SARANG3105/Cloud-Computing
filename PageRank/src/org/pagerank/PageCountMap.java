/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//this class contains the mapper for calculating the number of pages in input file.

public class PageCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one  = new IntWritable( 1);
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String line= lineText.toString();

		if(!line.isEmpty()){							//it just checks if line is not empty 	
			context.write(new Text("titles"), one);		//and writes the non-empty lines only, for reducer to process.
														//it writes output as [titles	1] for each line 
		}

	}
}
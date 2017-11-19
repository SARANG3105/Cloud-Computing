/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//mapper for sorting the output of final page rank
public class SortingMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException  {
		String line= lineText.toString();
		String[] page=line.split("\t");
		String[] rank= page[1].split("=====");
		double rank_fl= Double.parseDouble(rank[1]);
		//output is pagerank as key and page as value
		context.write(new DoubleWritable(rank_fl),new Text(page[0]));
	}
}
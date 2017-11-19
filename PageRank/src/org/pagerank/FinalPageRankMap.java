/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper for calculating the final page rank 
public class FinalPageRankMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException { 

		String line= lineText.toString();
//splitting the line at tab which gives [page, o1&&&&&o2=====initRank]
		String [] splitLine= line.split("\t");
		//splitting the line at tab which gives [o1&&&&&o2,initRank]
		String[] initPr= splitLine[1].toString().split("=====");
	
		String[] outlinks;
	//condition checks if array initPr at index[0] contains delimiter &&&&& if it does writes ouput of form
		//[page	@:	o1&&&&&o2]
		//[o1	initRank	numberOfOutlinks]
		//[o2	initRank	numberOfOutlinks]
			if(initPr[0].contains("&&&&&")){
				outlinks= initPr[0].toString().split("&&&&&");
				context.write(new Text(splitLine[0]), new Text ("@:"+"\t"+initPr[0]));
				for(String links: outlinks){
					
					context.write(new Text(links), new Text(String.valueOf(initPr[1]+"\t"+outlinks.length)));
				}
//condition checks if  splitline array contains===== and initPr[0] is empty
//inputs of form [page =====rank] handled ie, page with no outlinks
//output [page @:	empt]
	//	[empt	empt	initPR]	
			}else if(splitLine[1].contains("=====")&&initPr[0].isEmpty() ){
				context.write(new Text(splitLine[0]), new Text ("@:"+"\t"+"empt"));
				context.write(new Text("empt"), new Text("empt"+"\t"+initPr[1]));
				return;
//condition for pages with 1 outlink
// emitting [page	@:	outlink]
			//[outlink	initPr	1]
			}else if(!initPr[0].contains("&&&&&")&& initPr.length==2) {
				context.write(new Text(splitLine[0]), new Text ("@:"+"\t"+initPr[0]));
				context.write(new Text(initPr[0]), new Text(String.valueOf(initPr[1]+"\t"+1)));
			}
		}
	}


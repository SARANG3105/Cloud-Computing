/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */
package org.pagerank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//This is the mapper for assigning initial pageRanks to each page.
public class InitialPageRankMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException { 
		String linea = lineText.toString();
		String line= linea.trim();
		//String[] title= line.split("\t");			
		Pattern linkPat = Pattern .compile("\\[\\[.*?]\\]");			
		String outlinks="";
		Matcher m = linkPat.matcher(line); // extract outgoing links
		while(m.find()) { // loop on each outgoing link
			String url = m.group().replace("[[", "").replace("]]", ""); // drop the brackets and any nested ones if any
			if(!url.isEmpty()) {
				if(outlinks.equals("")){
					outlinks+=url;							
				}else{
					outlinks+="&&&&&"+url;		//appends &&&&& delimiter between the outlinks

				}
			}
		}

		if(line.contains("<title>") && line.contains("</title>")){  //checks if line contains title tags.
			String[] title= line.split("</title>");					//split the line at </title>
			String fullTitle= title[0].substring(7);				//select substring after 7 letters removing the initial <title>
			//if(!outlinks.equals("")){												
				context.write(new Text(fullTitle), new Text(outlinks));//output is like [pageTitle	oulink1&&&&&outlink2]
			//}
		}
	}
}
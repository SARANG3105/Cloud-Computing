/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */

package org.myorg;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;


public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Search.class);
	private static String fileName = null;
	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new Search(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {


		Configuration c= new Configuration();
//scanner created to get search query.
		Scanner s= new Scanner(System.in);
		System.out.println("Search Query");
		
		String a =s.nextLine();
		c.set("query", a);
		s.close();
		Job jobQry  = Job .getInstance(c, " wordcount ");
		jobQry.setJarByClass( this .getClass());
		//Job 1 for first map reduce to execute search
		FileInputFormat.addInputPaths(jobQry,  args[0]);
		FileOutputFormat.setOutputPath(jobQry,new Path (args[1]) );
		jobQry.setMapperClass( QueryMap .class);
		jobQry.setReducerClass( QueryReduce .class);
		jobQry.setOutputKeyClass( Text .class);
		jobQry.setOutputValueClass( DoubleWritable .class);



		return jobQry.waitForCompletion( true)  ? 0 : 1; 
	}
// Mapper to check if the input query matches the word in tfidf output file. It creates the desired input for reducer of form 'filename	tfidf'.	
	public static class QueryMap extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
		private final static DoubleWritable one  = new DoubleWritable( 1);

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String a=context.getConfiguration().get("query");
	try{
		String ac= a.toLowerCase();
			String[] b= ac.split("\\s");	
            
			String[] wordFileTf=lineText.toString().split("\t");			
			String[] wordFile=wordFileTf[0].toString().split("#####");
			
			for(int i=0;i<=b.length;i++){
			if(wordFile[0].equals(b[i])){
				
				Text file= new Text(wordFile[1]);
				Double tf= new Double(Double.valueOf(wordFileTf[1]));
				context.write(file, new DoubleWritable(tf));
			}
		}
	}catch(ArrayIndexOutOfBoundsException er){
		
	}
		}}
// the reducer gets the output of mapper and sums the tfidf of occurrences in each file and outputs 'filename totalTFIDF'
	public static class QueryReduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {

		public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			double sum  = 0; 
			for(DoubleWritable d: counts){
				sum  += d.get();			

			}
			context.write(word,  new DoubleWritable(sum));
		}
	}
}

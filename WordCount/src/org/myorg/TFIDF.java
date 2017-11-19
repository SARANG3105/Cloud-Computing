
package org.myorg;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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


public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( TFIDF.class);
	private static String fileName = null;
	public static void main( String[] args) throws  Exception { 
		int res  = ToolRunner .run( new TFIDF(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

//Job 1 for first Map reduce.
		Job job_tf  = Job .getInstance(getConf(), " wordcount ");
		job_tf.setJarByClass( this .getClass());		
		FileInputFormat.addInputPaths(job_tf,  args[0]);
		FileOutputFormat.setOutputPath(job_tf,new Path ("/tmpojf") );
		job_tf.setMapperClass( TFMap .class);
		job_tf.setReducerClass( TFReduce .class);
		job_tf.setOutputKeyClass( Text .class);
		job_tf.setOutputValueClass( DoubleWritable .class);
		
		job_tf.waitForCompletion(true); 

//Code to calculate total number of files for TFIDF calculation.
		Configuration c= new Configuration();		
		org.apache.hadoop.fs.FileSystem fs= org.apache.hadoop.fs.FileSystem.get(c); 
		Path p= new Path(args[0]);
		ContentSummary cs= fs.getContentSummary(p);
		long totalfiles=cs.getFileCount();
		c.setInt("TotalFiles",(int)totalfiles);

//Job 2 for second map reduce.			
		Job job_idf  = Job .getInstance(c, " TFIDF ");
		job_idf.setJarByClass( this .getClass());
		FileInputFormat.setInputPaths(job_idf,new Path("/tmpojf") );
		FileOutputFormat.setOutputPath(job_idf,new Path(args[ 1]) );
		job_idf.setMapperClass( TFIDFMap .class);
		job_idf.setReducerClass( TFIDFReduce .class);
		job_idf.setOutputKeyClass( Text .class);
		job_idf.setOutputValueClass( Text .class);

		return job_idf.waitForCompletion( true)  ? 0 : 1; 
	}

//First Map for creating the required output  for reduce of form 'word#####filename  wordfrequency'.	
static class TFMap extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
		private final static DoubleWritable one  = new DoubleWritable( 1);
		private Text word  = new Text();


		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			String file= ((FileSplit) context.getInputSplit()).getPath().getName();


			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
					continue;
				}

				String NewWord= new String(word.toString());
				Text currentWord = new Text(NewWord.toLowerCase()+"#####"+file);          

				context.write(currentWord, one);

			}
		}
	}
//Reducer for the first mapper which calculates the TermFrequency using formula, WF(t,d) the reducer writes the output to temp output folder which next map uses.
	public static class TFReduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {

		public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0; double wf=0.0;
			for ( DoubleWritable count  : counts) {
				sum  += count.get();
				if(sum>0){
					wf= Math.log10(10)+ Math.log10(sum);
				}else {
					wf=0;
				}
			}
			context.write(word,  new DoubleWritable(wf));
		}
	}
//Second Mapper to calculate the TFIDF of each word it creates input for mapper of format <"word​ ​",​ ​"filename=TF”>​ ​

	public static class TFIDFMap extends Mapper<LongWritable ,  Text ,  Text ,  Text > {



		public void map(LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {


			String[] wordFileTf= lineText.toString().split("\t");

			String[] wordFile= wordFileTf[0].toString().split("#####");
			System.out.println("length"+wordFile.length);
			Text wordName= new Text(wordFile[0]);
			try{
				String a=wordFile[1].toString()+"="+wordFileTf[1];

				Text ab = new Text(a);			
				//System.out.println(a);
				context.write(wordName,ab);	
			}catch(ArrayIndexOutOfBoundsException a){
				System.out.println("hehe");
			}
		}
	}
//Reducer for second mapper calculates the IDF of tf outputs from mapper.
	public static class TFIDFReduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {

		public void reduce(Text word,Iterable<Text > fileTf,   Context context)
				throws IOException,  InterruptedException {
			
			int filesWithWord = 0;
			Map<String,Double> kv= new HashMap<String,Double>();
			for(Text itr: fileTf){

				String[] file_tf= itr.toString().split("=");
				
				filesWithWord++;
				kv.put(file_tf[0], Double.valueOf(file_tf[1]));
			}
				int totalfiles= context.getConfiguration().getInt("TotalFiles",0);
		
				Double idf= Math.log10(1+totalfiles/filesWithWord);
				for(String a: kv.keySet()){
					Text newWord= new Text(word+"#####"+a);
					Double tfidf= kv.get(a)*idf; 
					context.write(newWord,  new DoubleWritable(tfidf));
				
			}
		}
	}
}

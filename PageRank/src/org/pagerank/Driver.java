/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */

package org.pagerank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



/*Driver class contains all the jobs required to calculate sorted page rank values*/

public class Driver extends Configured implements Tool {
	private static final Logger LOG = Logger .getLogger( Driver.class);
	private static String fileName = null;
	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new Driver(), args);
		System .exit(res);
	}



	//This method runs all the methods containing jobs for each mapreduce. 
	public int run( String[] args) throws  Exception {
		Configuration conf= new Configuration();
		FileSystem fs= FileSystem.get(conf);
		boolean execute= PageCount(args); 			//Calls the PageCount method. 
		execute= InitialPageRank(args);	
		try{
			for(int i=0;i<10;i++){						//for loop to run the finalpagerank method for 10 iterations
				String input= args[1]+"-fpr"+i;			
				String output = args[1]+"-fpr"+(i+1);
				execute= FinalPageRank(input,output);
				fs.delete(new Path(input),true);		//deleting the previous iteration folder created which keeps just the 
														//10 th iteration folder and deletes the rest.
			}
			String input= args[1]+"-fpr10";
			String output= args[1]+"-sorted-ranks";
			
			execute= Sorting(input,output);
			fs.delete(new Path(input),true);
		}catch(ArrayIndexOutOfBoundsException e){
			System.out.println("arry");
		}
		return execute? 0 : 1 ;
	}



	// Method for calculating the number f pages which are given by number of lines present in the file.
	public boolean PageCount (String[] args) throws Exception{
		Job job  = Job .getInstance(getConf(), " PageCount ");
		job.setJarByClass( this .getClass());
		LOG.info("JOB FOR CALCULATING NUMBER OF PAGES START");
		FileInputFormat.addInputPaths(job,  args[0]);			//the input file path goes here
		FileOutputFormat.setOutputPath(job,  new Path(args[ 1]+"-pagecount"));	//the output path appends the pagecount  TO 
																				//the initial output path and creates pagecount folder
		job.setMapperClass( PageCountMap .class);
		job.setReducerClass( PageCountReduce .class);
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion( true);
	}



	//Method for providing initial page ranks to all the pages which is 1/N, N being number of pages
	public boolean InitialPageRank (String[] args) throws Exception{
		Configuration conf= new Configuration();
		Path pt= new Path(args[1]+"-pagecount/part-r-00000"); //this path reads the PageCount job output 
		Path pt1= new Path(args[1]+"-pagecount");				
		FileSystem fs= FileSystem.get(conf);
		InputStreamReader isr=new InputStreamReader(fs.open(pt));
		BufferedReader br= new BufferedReader(isr);
		
		double N=0.0;String str;
		while((str=br.readLine())!=null){						//bufferedreader used to read the file with number of pages 
			String[] line= str.split("\t");		//which is used by the InitialPageRank MapReduce.
			N= Double.parseDouble(line[1]);
			break;
		}
		fs.delete(pt1,true);					// deleting the page count folder created after reading its input.
		br.close();
		conf.setDouble("N", N);
		LOG.info("JOB FOR ASSIGNING INITIAL PAGE RANK");
		Job job1  = Job .getInstance(conf, " InitPageRank ");
		job1.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[ 1]+"-fpr0"));
		job1.setMapperClass( InitialPageRankMap .class);
		job1.setReducerClass( InitialPageRankReduce .class);
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass(Text.class);

		return  job1.waitForCompletion( true);
	}




	// MEthod to calculate final page ranks for each page 
	public boolean FinalPageRank (String input,String output) throws Exception{
		LOG.info("JOB FOR CALCULATING FINAL PAGE RANK");
		Job job2  = Job .getInstance(getConf(), " FinalPageRank ");
		job2.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(job2,  input);		//input is created in run method and passed here.
		FileOutputFormat.setOutputPath(job2,  new Path(output)); //output path also created in run method

		job2.setMapperClass( FinalPageRankMap .class);

		job2.setReducerClass( FinalPageRankReduce .class);
		job2.setOutputKeyClass( Text .class);
		job2.setOutputValueClass(Text.class);		
		return job2.waitForCompletion( true);
	}



	// Method for sorting the output of finalpagerank job in descending order.
	public boolean Sorting (String input,String output) throws Exception{
		LOG.info("JOB FOR SORTING PAGE RANK");
		Job job3  = Job .getInstance(getConf(), " Sorting ");
		job3.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(job3,  input);
		FileOutputFormat.setOutputPath(job3,  new Path(output));
		job3.setSortComparatorClass(customComparator.class);	//The custom comparator class created below assigned to job 3 
																//for sorting the page ranks.
		job3.setMapperClass( SortingMap .class);					
		job3.setReducerClass( SortingReduce .class);
		
		job3.setOutputKeyClass( DoubleWritable .class);
		job3.setOutputValueClass(Text.class);		 
		return job3.waitForCompletion( true);
	}
}

//a custom comparator to sort the values in descending order.
	 class customComparator extends WritableComparator{
		protected customComparator(){
			super(DoubleWritable.class,true);
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable val1, WritableComparable  val2){
			DoubleWritable x= (DoubleWritable) val1;
			DoubleWritable y= (DoubleWritable) val2;
			int out=x.compareTo(y);
			return out*-1;
		}
	}
	



/* Submitted by: Sarangdeep Singh
 * email id: ssingh53@uncc.edu
 */

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);
   private static String fileName = null;
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " wordcount ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1; 
   }
 // The mapper for this class reads the input files and splits them word by word and forms input for mapper of form word#####filename	1  
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      private Text word  = new Text();
       

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         String file= ((FileSplit)context.getInputSplit()).getPath().getName();


         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
          
            String NewWord= new String(word.toString());
            Text currentWord = new Text(NewWord+"#####"+file+"\t");          
            
            context.write(currentWord, one);
         
            
         }
      }
   }
// The reducer gets the ouput of mapper as input and sums the total occurences of a word one file at a time. it also calculates the
//term frequency of each word WF(t,d)​ ​=​ ​1​ ​+​ ​log​10​(TF(t,d))​ ​ ​if​ ​TF(t,d)​ ​>​ ​0,​ ​and​ ​0​ ​otherwise where TF is total occurences of each word.

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      
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
}

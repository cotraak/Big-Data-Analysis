// Design and execute a MapReduce program to produce the frequencies of all the words in the book collection, retaining only those words whose frequencies are greater than 5. Submit the following:
// a.	Your commented code for the Mapper and Reducer
// b.	First 50 words and their frequencies from your program’s output file.
// c.	Last 50 words and their frequencies from your program’s output file.

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenMapper //mapping each token to 1.
            extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); //variable 'one' of type IntWritable assigned to 1.
    private Text word = new Text(); //variable 'word' of type Text.

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //initiating string tokenizer
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        //while there are more tokens.
        word.set(itr.nextToken());//word assigned to the token and
        context.write(word, one);//the key value pair <word,1> is produced.
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> { //<keyin, valuein, keyout, valueout>
    private IntWritable result = new IntWritable();//initializing 'result'

    public void reduce(Text key, Iterable<IntWritable> values,Context context)
                  throws IOException, InterruptedException {   //reduce function
      int sum = 0;     //intializing sum
      for (IntWritable val : values) {
        sum += val.get();       //adding all the ones of the same word.
      }
      result.set(sum);  //assigning sum to result
      context.write(key, result); // <(word1,word2),frequency>
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

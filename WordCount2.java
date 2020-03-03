// Design and execute a MapReduce program to produce frequencies of all 2-gram word-pairs in the book collection, retaining only those 2-grams whose frequencies are greater than 5. Submit the following:
// a.	Your commented MapReduce program to produce the frequencies of all the 2-grams in all the books.
// b.	First 50 2-grams and their frequencies from your program’s output file.
// c.	Last 50 2-grams and their frequencies from your program’s output file.


import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Object;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

  public static class TokenMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);// initializing 1.
    private String word1 = new String();// initializing word1,word2, word3 as strings.
    private String word2 = new String();
    private String word3 = new String();
    private Text word4 = new Text();// initializing word4 as text
    private String p = new Text();// initializing p

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      p = itr.nextToken(); // assigning p to the first token
      while (itr.hasMoreTokens()) {
        word1 = p; //now word1 = first token
        word2 = itr.nextToken();// word2 = second token
        word3 = word1.toString() + " " + word2.toString(); // combining both words with space.
        word4.set(word3);// assigning word3 to word4 as Text.
        context.write(word4, one); // <bigram,1>
        p = word2; // assigning p to the second word so that it is assigned to the first word next.
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
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

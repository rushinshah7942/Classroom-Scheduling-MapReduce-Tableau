import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SeatsUtilization {

  public static class Mapper1
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(",");
    	if(fields[3].equals("Unknown")||fields[1].equals("")|| !StringUtils.isNumeric(fields[1])||fields[9].equals("")||!StringUtils.isNumeric(fields[9]) || !StringUtils.isNumeric(fields[10]) || fields[10].equals("") ||fields[10].equals("0"))return;

	word.set(fields[3].split(" ")[1]+"_"+fields[1]);
	context.write(word,new DoubleWritable(Double.parseDouble(fields[9]) / Double.parseDouble(fields[10]) ));
    }
  }

  public static class Reducer1
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	double sum = 0;
	int count = 0;

    	for(DoubleWritable val:values){
    		sum+=val.get();
		count+=1;
    	}
    	result.set(sum/count);
    	context.write(key,result);
    }
  }

public static class Mapper2
  extends Mapper<Object, Text, Text, DoubleWritable>{
  	Text word=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t");
	StringBuffer year = new StringBuffer();
	String course="";

	for(int i=0; i<fields[0].length(); ++i){
		if(fields[0].charAt(i) == '_'){
			course = fields[0].substring(i+1, fields[0].length());
			break;
		}
		year.append(fields[0].charAt(i));
	}

	String yearString = year.toString();

	String key1=yearString+"-"+(Integer.parseInt(yearString)+1)+"_"+course;
	String key2=Integer.toString(Integer.parseInt(yearString)-1)+"-"+yearString+"_"+course;
	word.set(key1);
	context.write(word,new DoubleWritable(Double.parseDouble(fields[1])));
	word.set(key2);
	context.write(word,new DoubleWritable(Double.parseDouble(fields[1])));
}
}

public static class Reducer2
  extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

public void reduce(Text key, Iterable<DoubleWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
	Iterator<DoubleWritable> iterator=values.iterator();
	double value1=iterator.next().get();
	if(!iterator.hasNext())return;
	double value2=iterator.next().get();
	value2=value2-value1;
	context.write(key,new DoubleWritable(value2));
}
}
 
  public static void main(String[] args) throws Exception {
	String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Seats Utilization (Actual Enrollment / Max No. of Students)for every courseID for every Year");
    job.setJarByClass(SeatsUtilization.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Seats Utilization (Actual Enrollment / Max No. of Students) difference for every courseID for consecutive years");
    job2.setJarByClass(SeatsUtilization.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);    
  }
}

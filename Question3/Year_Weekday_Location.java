import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Year_Weekday_Location {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(",");
    	if(fields[3].equals("Unknown") || fields[6].equals("UNKWN") || fields[5].equals("Unknown"))return;
	
	HashMap<Character, String> map = new HashMap<Character, String>();
	map.put('M',"Monday");
	map.put('T',"Tuesday");
	map.put('W',"Wednesday");
	map.put('H',"Thursday");
	map.put('R',"Thursday");
	map.put('F',"Friday");
	map.put('S',"Saturday");
	map.put('U',"Sunday");

	StringBuffer sb = new StringBuffer();
	for(int i=0; i<fields[5].length(); ++i){
		if(fields[5].charAt(i)==' '){
			break;
		}
		sb.append(fields[5].charAt(i));	
	}

	for(int i=0; i<fields[6].length(); ++i){
		if(! sb.toString().equals("Arr")){
			if(map.containsKey(fields[6].charAt(i))){
				word.set(fields[3].split(" ")[1]+"_"+map.get(fields[6].charAt(i))+"_"+sb.toString());
	    			context.write(word,new IntWritable(1));
			}
		}		
	}
    	
    }
  }

  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int sum=0;
    	for(IntWritable val:values){
    		sum+=val.get();
    	}
    	result.set(sum);
    	context.write(key,result);
    }
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, IntWritable>{
  	Text word=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t");
	StringBuffer year = new StringBuffer();
	String weekday="";

	for(int i=0; i<fields[0].length(); ++i){
		if(fields[0].charAt(i) == '_'){
			weekday = fields[0].substring(i+1, fields[0].length());
			break;
		}
		year.append(fields[0].charAt(i));
	}

	String yearString = year.toString();

	String key1=yearString+"-"+(Integer.parseInt(yearString)+1)+"_"+weekday;
	String key2=Integer.toString(Integer.parseInt(yearString)-1)+"-"+yearString+"_"+weekday;
	word.set(key1);
	context.write(word,new IntWritable(Integer.parseInt(fields[1])));
	word.set(key2);
	context.write(word,new IntWritable(Integer.parseInt(fields[1])));
}
}

public static class Reducer2
  extends Reducer<Text,IntWritable,Text,IntWritable> {

public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
	Iterator<IntWritable> iterator=values.iterator();
	int value1=iterator.next().get();
	if(!iterator.hasNext())return;
	int value2=iterator.next().get();
	value2=value2-value1;
	context.write(key,new IntWritable(value2));
}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get total number of classes on every WeekDay for every Year");
    job.setJarByClass(Year_Weekday_Location.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "get # of classes increasing between years (on particular WeekDay)");
    job2.setJarByClass(Year_Weekday_Location.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}

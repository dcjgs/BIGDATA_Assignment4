package mapreduce.demo.task1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Acadass42Task1Mapper  extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	IntWritable one = new IntWritable(1);
	Text company = new Text();
	
	
	



private boolean recordIsBad(Text record) {
	// return true if record is bad by your standards
	String valstr = record.toString();
	// NA not found, so this is a good record, -1 is returned when text not
	// found
	if (valstr.indexOf("NA") == -1)
		return false;
	else
		return true;

}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// String[] lineArray = value.toString().split("|");

		//get rid of the invalid records , so that they dont add to the counts.
		if (recordIsBad(value)) {

			context.getCounter("Bad Record Counter", "Containing NA").increment(1);
		} else {
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line, "|");

			// calculate the total units sold for each Company.
			// Here in data set no units sold are mentioned.Considering adding
			// company occurencies.
			// Company Name|Product Name|Size in inches|State|Pin Code|Price
			// Samsung|Optima|14|Madhya Pradesh|132401|14200
			String str = tokenizer.nextToken();
			System.out.println("From The Mapper=>" + str);
			// Text company = new Text(lineArray[0]);
			company.set(str);
			// company,1
			context.write(company, one);
		}
	}
}
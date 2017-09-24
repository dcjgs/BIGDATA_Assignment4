import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;



public class Ass41Mapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  private final static IntWritable one = new IntWritable(1);	 
  private Text word = new Text();
  private static final Logger _logger = Logger.getLogger(Ass41Mapper.class);
 
  
  @Override
  protected void map(LongWritable key, Text value, Context context) 
		  throws IOException, InterruptedException {

	    if(recordIsBad(value))
	    	_logger.info("Bad record");
	    else
	        context.write(word, one);

	}

	private boolean recordIsBad(Text record) {
		// return true if record is bad by your standards
		String valstr = record.toString();
		if (valstr.indexOf("NA") != -1)
			return false;
		return true;

	}
}

package nudt.ceaserz.hadoop_blazegraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HypernymMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String record = ivalue.toString();
		String[] line = record.split("\t");
		if (line.length == 11) {
			Text concept = new Text(line[0]);
			Text entity = new Text(line[1]);
			context.write(concept, entity);
		}
	}

}

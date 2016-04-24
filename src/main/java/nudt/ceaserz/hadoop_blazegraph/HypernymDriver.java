package nudt.ceaserz.hadoop_blazegraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HypernymDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "hypernym_blazegraph");
		job.setJarByClass(nudt.ceaserz.hadoop_blazegraph.HypernymDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(nudt.ceaserz.hadoop_blazegraph.HypernymMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(nudt.ceaserz.hadoop_blazegraph.HypernymReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://172.16.0.112:9000/data/probase"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://172.16.0.112:9000/user/data/probase_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}

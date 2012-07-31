package cn.ict.magicube.fs.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import cn.ict.magicube.fs.PartInfo;

public class NKFSTransformerInputFormat extends SequenceFileInputFormat<Text, PartInfo> {
		public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();

		/* only 1 input file */
		Path inDir = getInputPaths(job)[0];
		FileSystem fs = inDir.getFileSystem(conf);
		FileStatus[] inputFiles = fs.listStatus(inDir);
		Path inputFile = inputFiles[0].getPath();
		List<InputSplit> splits = new LinkedList<InputSplit>();
		SequenceFile.Reader in =
			new SequenceFile.Reader(conf, Reader.file(inputFile));

		long prev = 0L;
		final int opsPerTask = conf.getInt("nkfs.transformer.opspertask", 1);
		try {
			Text key = new Text();
			PartInfo value = new PartInfo();
			int count = 0;
			while (in.next(key, value)) {
				long curr = in.getPosition();
				long delta = curr - prev;
				if (++count > opsPerTask) {
					count = 0;
					splits.add(new FileSplit(inputFile, prev, delta, (String[]) null));
					prev = curr;
				}
			}
		} finally {
			in.close();
		}
		long remaining = fs.getFileStatus(inputFile).getLen() - prev;
		if (remaining != 0) {
			splits.add(new FileSplit(inputFile, prev, remaining, (String[]) null));
		}
		return splits;
	}
}

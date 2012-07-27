package cn.ict.magicube.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.net.URI;

import cn.ict.magicube.math.GaloisField;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NKFSTransformer {
	static final Log LOG = LogFactory.getLog(NKFSTransformer.class);
	
	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	public static class BlockInfo implements Writable {
		public Path path;
		public long offset;
		public long length;

		
		public BlockInfo(Path p, long s, long l) {
			path = p;
			offset = s;
			length = l;
		}
		
		public BlockInfo() {
			path = null;
			offset = length = -1L;
		}
		
		//////////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////////
		static {                                      // register a ctor
			WritableFactories.setFactory
			(BlockInfo.class,
					new WritableFactory() {
				public Writable newInstance() { return new BlockInfo(); }
			});
		}
		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, path.toUri().toString());
			Text.writeString(out, Long.toString(offset));
			Text.writeString(out, Long.toString(length));
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			path = new Path(URI.create(Text.readString(in)));
			offset = Long.parseLong(Text.readString(in));
			length = Long.parseLong(Text.readString(in));
		}
		
		@Override
		public String toString() {
			return path.toString() + ": " + Long.toString(offset) + " -- +" + Long.toString(length);
		}
	} 

	static class NKFSTransformerInputFormat extends
		SequenceFileInputFormat<Text, BlockInfo> {
		
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
				BlockInfo value = new BlockInfo();
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
	
	static class NKFSTransformerMapper extends Mapper<Text, BlockInfo, Text, Text> {
		static final Log LOG = LogFactory.getLog(NKFSTransformerMapper.class);
		
		long _offset;
		long _length;
		Path _originalPath;
		Path _partDir;
		
		private void makeOriginPart() throws IOException {
			/* create origin file */
			LOG.info("create origin file");
			String originFileName = "origin";
			short originRepl = (short)conf.getInt("nkfs.origin.replication", 1);
			
			Path originPartPath = NKFileSystem.resolvePath(_partDir, originFileName);

			LOG.info("writing to " + originPartPath.toUri());
			FSDataOutputStream out = null;
			FSDataInputStream in = null;
			try {
				in = baseFS.open(_originalPath);
				out = baseFS.create(originPartPath, originRepl);
				in.seek(_offset);

				long written = 0;
				byte[] buffer = new byte[1024 * 512];
				while (written < _length) {
					int len = in.read(buffer);
					if (len > 0)
						out.write(buffer, 0, len);
					written += len;
				}
			} finally {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			}
			LOG.debug("write to " + originPartPath.toUri() + " over");

			/* check length */
			if (baseFS.getFileStatus(originPartPath).getLen() != _length) {
				String errstr = String.format("write to %s failed: except write %d bytes but wrote %d bytes.",
						originPartPath.toUri(), _length, baseFS.getFileStatus(originPartPath).getLen());
				LOG.error(errstr);
				baseFS.delete(originPartPath, false);
				throw new IOException(errstr);
			}
		}
		
		
		private static int vectorMul(GaloisField GF, int[] v1, int[] v2) {
			assert v1.length == v2.length;
			int v = 0;
			for (int i = 0; i < v1.length; i++) {
				v = GF.add(v,
						GF.multiply((int)(v1[i] & 0xFF),
								    (int)(v2[i] & 0xFF))
					);
			}
			return v;
		}
		
		public void makeParities() throws IOException {
			LOG.info("create parities");
			int N = topFS.getN();
			int K = topFS.getK();
			short parityRepl = (short)conf.getInt("nkfs.parity.replication", 1);
			int parityShift = (short)conf.getInt("nkfs.parity.coding.shift", 3);

			GaloisField GF = GaloisField.getInstance();
			int[][] pivots = new int[N][];
			for (int i = 0; i < N; i++) {
				pivots[i] = GF.makePivot(i + parityShift, K);
			}
			
			OutputStream[] paritiesOS = new OutputStream[N];
			FSDataInputStream in = null;
			Path[] paritiesPaths = new Path[N];

			for (int i = 0; i < N; i++) {
				paritiesOS[i] = null;
			}
			
			try{
				in = baseFS.open(_originalPath);
				in.seek(_offset);
				for (int i = 0; i < N; i++) {
					String parityFileName = "parity_" + Integer.toString(i + parityShift);
					paritiesPaths[i] = NKFileSystem.resolvePath(_partDir, parityFileName);
					LOG.info("writing to " + paritiesPaths[i].toUri());
					paritiesOS[i] = baseFS.create(paritiesPaths[i], parityRepl);
				}
				
				int bytesProcessed = 0;
				int[] buf = new int[K];
				while (bytesProcessed < _length) {
					// read K bytes
					long leftBytes = _length - bytesProcessed;
					int i;
					for (i = 0; i < Math.min(K, leftBytes); i++) {
						buf[i] = in.read() & 0xFF;
					}
					bytesProcessed += i;
					for (i = 0; i < N; i++) {
						int v = vectorMul(GF, buf, pivots[i]);
						paritiesOS[i].write(v);
					}
				}
			} finally {
				if (in != null)
					in.close();
				for (OutputStream o : paritiesOS) {
					if (o != null)
						o.close();
				}
			}
			
			/* check length */
			long expected_len = (_length + K) / K;
			boolean error = false;
			for (int i = 0; i < N; i++) {
				if (baseFS.getFileStatus(paritiesPaths[i]).getLen() != expected_len) {
					String errstr = String.format("write to %s failed: except write %d bytes but wrote %d bytes.",
							paritiesPaths[i].toUri(), expected_len, baseFS.getFileStatus(paritiesPaths[i]).getLen());
					LOG.error(errstr);
					baseFS.delete(paritiesPaths[i], false);
					error = true;
				}
			}
			if (error)
				throw new IOException("unexpccted parity length");
		}
		
		public void map(Text key, BlockInfo value, Context context)
			throws IOException, InterruptedException 
		{
			LOG.info("NKFSTransformerMapper run for " + value.toString());
			initialize(context.getConfiguration());
			LOG.debug("initialize complete");
			
			_originalPath = value.path;
			_offset = value.offset;
			_length = value.length;
			
			/* create parity dir */
			Path topPath = topFS.convertFromBaseOrigin(_originalPath);
			NKFileSystem.PathTranslator ptran = topFS.new PathTranslator(topPath);
			Path parityDir = ptran.getBaseParityDirPath();
			baseFS.mkdirs(parityDir);
			
			/* create part dir */
			String partDirName = String.format("part-%d-%d", _offset, _length);
			_partDir = NKFileSystem.resolvePathToDir(parityDir, partDirName);
			LOG.debug("_partDir=" + _partDir.toUri());

			boolean origin = key.toString().equalsIgnoreCase("origin");
			
			if (origin) {
				makeOriginPart();
			} else {
				makeParities();
			}
		}
	}

	
	static Configuration conf = null;
	static NKFileSystem topFS = null;
	static FileSystem baseFS = null;
	static Path jobInputPath = null;
	static Path jobOutputDirPath = null;
	static FileStatus[] originFiles = null;
	
	public static void initialize(Configuration conf) throws IOException {
		if (conf != null)
			NKFSTransformer.conf = conf;
		else
			NKFSTransformer.conf = new Configuration();
		topFS = new NKFileSystem();
		topFS.initialize(topFS.getUri(), new Configuration());
		baseFS = topFS.getBaseFileSystem();
	}
	
	
	static void getAllOriginFiles() throws FileNotFoundException, IOException {
		NKFileSystem.PathTranslator ptran =
			topFS.new PathTranslator(new Path("/"));
		LOG.info("iterate over " + ptran.getBaseOriginPath());

		LinkedList<FileStatus> l = new LinkedList<FileStatus>();
		LinkedList<Path> pendingPath = new LinkedList<Path>();
		pendingPath.add(ptran.getBaseOriginPath());
		while (pendingPath.size() > 0) {
			Path currentPath = pendingPath.pop();
			FileStatus[] status = baseFS.listStatus(currentPath);
			for (FileStatus s : status) {
				if (s.isDirectory())
					pendingPath.add(s.getPath());
				if (s.isFile()) {
					l.add(s);
					LOG.debug("Find File " + s.getPath());
				}
			}
		}
		java.util.Collections.shuffle(l);
		//originFiles = l.toArray();
		originFiles = new FileStatus[l.size()];
		for (int i = 0; i < originFiles.length; i++) {
			originFiles[i] = l.pop();
		}
	}

	static void createJobInputFile() throws Exception {
		baseFS.mkdirs(jobInputPath.getParent());
		if (baseFS.exists(jobInputPath))
			throw new Exception("Another NKFS transforming is working. try to remove " + jobInputPath.toString());
		
		LOG.debug("creating " + jobInputPath);
		FSDataOutputStream out = baseFS.create(jobInputPath);
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(conf,
					Writer.stream(out),
					Writer.keyClass(Text.class),
					Writer.valueClass(NKFSTransformer.BlockInfo.class),
					Writer.compression(SequenceFile.CompressionType.NONE));
			
	        for (FileStatus stat : originFiles) {
	        	BlockLocation blocks[] = baseFS.getFileBlockLocations(stat, 9, stat.getLen());
	        	for (BlockLocation block : blocks) {
	        		BlockInfo info = new BlockInfo(
	        				baseFS.makeQualified(stat.getPath()),
	        				block.getOffset(), block.getLength());
	        		writer.append(new Text("origin"), info);
	        		LOG.info("append to seq file: origin : " + info.toString());
	        		writer.append(new Text("parities"), info);
	        		LOG.info("append to seq file: parities : " + info.toString());
	        	}
	        }
		} finally {
			if (writer != null) {
				writer.close();
        		LOG.debug("writer closed");
			}
		}
		out.close();
	}
	
	static long now() {
		return System.currentTimeMillis();
	}
	
	static Job createJob() throws IOException {
		final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		String jobName = "nkfs_transform_" + dateForm.format(new Date(now()));
		
		Job job = Job.getInstance(conf, jobName);
		
	    job.setSpeculativeExecution(false);
	    job.setJarByClass(NKFSTransformer.class);
	    job.setInputFormatClass(NKFSTransformerInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(NKFSTransformerMapper.class);
	    FileInputFormat.setInputPaths(job, jobInputPath);
	    FileOutputFormat.setOutputPath(job, jobOutputDirPath);
	    job.setNumReduceTasks(0);
		return job;
	}
	
	public static void main(String[] args) {
		try {
			initialize(null);
			getAllOriginFiles();
			LOG.debug("conf = " + conf);
			jobInputPath = new Path(conf.get("nkfs.working.file", "/tmp/working/in"));
			jobInputPath = new Path(
					new URI(baseFS.getScheme(),
					jobInputPath.toUri().getPath(),
					null));
			
			createJobInputFile();
			jobOutputDirPath = new Path(conf.get("nkfs.working.output.dir",
					"/tmp/working/out"));
			jobOutputDirPath = new Path(
					new URI(baseFS.getScheme(),
							jobOutputDirPath.toUri().getPath(),
					null));
			//baseFS.mkdirs(jobOutputDirPath);
			if (baseFS.exists(jobOutputDirPath))
				baseFS.delete(jobOutputDirPath, true);
			
			////////////
			// prepare transforming
			////////////
			for (FileStatus stat : originFiles) {
				topFS.prepareTransformOriginPath(stat.getPath());
			}
			
			//////
			// create mapreduce job
			//////
			try {
				Job job = createJob();
				job.submit();
				job.waitForCompletion(true);
			} catch (Exception e) {
				e.printStackTrace();
			}

			baseFS.delete(jobInputPath, false);
			baseFS.delete(jobOutputDirPath, true);
			LOG.info("job finished");
			
			LOG.info("remove write permission for original files," +
					"then remove origin part");
			for (FileStatus stat : originFiles) {
				topFS.finishTransformOriginPath(stat.getPath());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}

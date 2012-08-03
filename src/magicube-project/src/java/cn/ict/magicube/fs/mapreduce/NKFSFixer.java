package cn.ict.magicube.fs.mapreduce;


import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.ict.magicube.fs.NKFileSystem;
import cn.ict.magicube.fs.PartInfo;
import cn.ict.magicube.fs.PartInfo.ParityInfo;
import cn.ict.magicube.fs.PathUtils;
import cn.ict.magicube.fs.NKFileSystem.NKPathTranslator;


public class NKFSFixer extends NKFSUtil {
	public NKFSFixer() throws IOException {
		super();
	}

	static final Log LOG = LogFactory.getLog(NKFSFixer.class);
	public static void main(String[] args) throws Exception {
		NKFSFixer fixer = new NKFSFixer();
		fixer.doFix();
	}

	private Path _jobInputPath;
	private Path _jobOutputDirPath;
	private Job _job;
	
	private Set<PartInfo> _lostOriginFiles = new HashSet<PartInfo>();
	private Set<ParityInfo> _lostParityFiles = new HashSet<ParityInfo>();
	
	private Collection<NKPathTranslator> getRaidedTopFiles() throws IOException {
		LinkedList<NKPathTranslator> l = new LinkedList<NKPathTranslator>();
		LinkedList<Path> pendingPath = new LinkedList<Path>();
		pendingPath.add(new Path("/"));
		while (pendingPath.size() > 0) {
			Path currentPath = pendingPath.pop();
			FileStatus[] status = topFS.listStatus(currentPath);
			for (FileStatus s : status) {
				LOG.debug("Check File " + s.getPath());
				if (s.isDirectory())
					pendingPath.add(s.getPath());
				if (s.isFile()) {
					NKFileSystem.NKPathTranslator ptran = topFS.new NKPathTranslator(s.getPath());
					if (ptran.isRaidedFile()) {
						l.add(ptran);
						LOG.debug("Find File " + s.getPath());
					}
				}
			}
		}
		java.util.Collections.shuffle(l);
		return l;
	}
	
	
	private boolean isRecoverable(PartInfo part) throws IOException {
		return (part.getExistingParities(topFS).length >= K);
	}

	
	private void checkAndList() throws IOException {
		Collection<NKPathTranslator> raidedTopFiles = getRaidedTopFiles();
		for (NKPathTranslator ptran : raidedTopFiles) {
			PartInfo[] parts = topFS.getParts(ptran);
			for (PartInfo p : parts) {
				if (!baseFS.exists(p.getOriginFile(topFS))) {
					LOG.info(String.format("origin of part %s lost", p));
					if (isRecoverable(p)) {
						_lostOriginFiles.add(new PartInfo(p));
					} else {
						LOG.warn(String.format(
								"WARNING: part %s is not recoverable",
								p));
						continue;
					}
				}

				for (int parityNum : getParityNums()) {
					if (!p.checkParityFile(topFS, parityNum)) {
						LOG.info(String.format("parity %d of part %s lost",
								parityNum, p));
						ParityInfo pi = p.new ParityInfo(topFS, parityNum);
						_lostParityFiles.add(pi);
					}
				}
			}
		}
	}

	
	private void createJobFile() throws IOException {
		Map<PartInfo, String> operations = new HashMap<PartInfo, String>();
		for (PartInfo p : _lostOriginFiles) {
			if (!operations.containsKey(p))
				operations.put(p, "FIX");
			String op = operations.get(p) + " ORIGIN";
			operations.put(p, op);
		}
		
		for (ParityInfo parity : _lostParityFiles) {
			PartInfo part = parity.getOwner();
			if (!operations.containsKey(part))
				operations.put(part, "FIX");
			String op = operations.get(part) + " " + Integer.toString(parity._parityNum);
			operations.put(part, op);
		}
		
		baseFS.mkdirs(_jobInputPath.getParent());
		if (baseFS.exists(_jobInputPath))
			throw new IOException("Another NKFS fixer is working. try to remove " + _jobInputPath.toString());
		LOG.debug("creating " + _jobInputPath);
		FSDataOutputStream out = baseFS.create(_jobInputPath);
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(conf,
					Writer.stream(out),
					Writer.keyClass(Text.class),
					Writer.valueClass(PartInfo.class),
					Writer.compression(SequenceFile.CompressionType.NONE));
			
			for (PartInfo part : operations.keySet()) {
				String op = operations.get(part);
				writer.append(new Text(op), part);
	       		LOG.info(String.format(
	       				"append to seq file: %s, %s",
	       				op, part));
			}
		} finally {
			if (writer != null) {
				writer.close();
        		LOG.debug("writer closed");
			}
			out.close();
		}		
	}
	
	private void createJob() throws IOException {
		final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		String jobName = "nkfs_fixer_" + dateForm.format(new Date(now()));
		Job job = Job.getInstance(conf, jobName);
		
		job.setSpeculativeExecution(false);
	    job.setJarByClass(NKFSFixer.class);
	    job.setInputFormatClass(NKFSTransformerInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(NKFSTransformerMapper.class);
	    FileInputFormat.setInputPaths(job, _jobInputPath);
	    FileOutputFormat.setOutputPath(job, _jobOutputDirPath);
	    job.setNumReduceTasks(0);
	    _job = job;
	}

	public void doFix() throws IOException {
		LOG.info(String.format("Begin fixing nkfs %s (based on %s)",
				topFS.getUri(),
				baseFS.getUri()));
		Set<Path> corruptedFiles = new HashSet<Path>();
		RemoteIterator<Path> iter = baseFS.listCorruptFileBlocks(PathUtils.BASE_PARITIES_DIR);
		
		while (iter.hasNext()) {
			Path corruptedFile = iter.next();
			LOG.info(String.format("file %s corrupted", corruptedFile));
			corruptedFiles.add(corruptedFile);
		}
		LOG.info("iterate over");
		
		boolean successful = false;
		try {
			_jobInputPath = new Path(conf.get("nkfs.working.file.fixer", "/tmp/working/in-fix"));
			_jobInputPath = new Path(
					new URI(baseFS.getScheme(),
							_jobInputPath.toUri().getPath(),
							null));
			_jobOutputDirPath = new Path(conf.get("nkfs.working.output.dir.fixer",
				"/tmp/working/out-fix"));
			_jobOutputDirPath = new Path(
					new URI(baseFS.getScheme(),
							_jobOutputDirPath.toUri().getPath(),
							null));
			if (baseFS.exists(_jobOutputDirPath))
				baseFS.delete(_jobOutputDirPath, true);

			// remove all corrupted files
			for (Path p : corruptedFiles) {
				baseFS.delete(p, false);
			}
			
			checkAndList();
			createJobFile();
			createJob();
			successful = _job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		} finally {
			if (_jobInputPath != null)
				baseFS.delete(_jobInputPath, true);
			if (_jobOutputDirPath != null)
				baseFS.delete(_jobOutputDirPath, true);
		}
		
		if (successful) {
			LOG.info("NKFS Fixer finished");
		} else {
			LOG.info("NKFS Fixer failed");
		}
	}
}

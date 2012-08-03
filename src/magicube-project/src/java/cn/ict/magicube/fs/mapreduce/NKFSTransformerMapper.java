package cn.ict.magicube.fs.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.ict.magicube.fs.ParityCreator;
import cn.ict.magicube.fs.PartInfo;
import cn.ict.magicube.fs.PartInfo.ParityInfo;
import cn.ict.magicube.math.RaidAlgorithm;
import cn.ict.magicube.math.RaidNK;


public class NKFSTransformerMapper extends Mapper<Text, PartInfo, Text, Text> {
	static final Log LOG = LogFactory.getLog(NKFSTransformerMapper.class);
	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	private NKFSUtil _util;
	private Configuration _conf;
	private RaidAlgorithm _raid;
	private ParityCreator _parityCreator;

	private void createParities(int[] ops, PartInfo part) throws IOException {
		LOG.info("creating parities for " + part);
		// layout stuff
		Path partDir = part.getPartDir(_util.topFS);
		LOG.info("partDir=" + partDir.toUri());
		_util.baseFS.mkdirs(partDir);

		// create IS and OS
		OutputStream originOS = null;
		OutputStream[] paritiesOS = null;
		FSDataInputStream is = null;

		List<Integer> l_parityNums = new LinkedList<Integer>();

		try {	
			if (part.isSrcTopPath) {
				is = _util.topFS.open(part.srcPath);
			} else {
				is = _util.baseFS.open(part.srcPath);
			}
			is.seek(part.srcOffset);

			for (int op : ops) {
				if (op == 0)
					_parityCreator.addOutputPath("origin", true);
				else {
					l_parityNums.add(op);
					_parityCreator.addOutputPath(String.format("parity_%d", op), false);
				}
			}

			originOS = _parityCreator.getOriginOutputStream();
			paritiesOS = _parityCreator.getOutputStreams();
			int[] parityNums = new int[l_parityNums.size()];

			for (int i = 0; i < l_parityNums.size(); i++) {
				parityNums[i] = l_parityNums.get(i);
			}

			_raid.encode(is, paritiesOS, parityNums, part.length, originOS);
			_parityCreator.reset();

		} finally {
			if (is != null)
				is.close();
			if (originOS != null)
				originOS.close();
			if (paritiesOS != null) {
				for (OutputStream os : paritiesOS)
					os.close();
			}
		}

	}

	private static class KeyInfo {
		final boolean isFixer;
		final boolean recoverOrigin;
		final int[] parityNums;
		KeyInfo(String key) {
			LOG.info("parsing key " + key);
			String[] toks = key.trim().split(" ");

			boolean isFixer_ = false;
			boolean recoverOrigin_ = false;
			List<Integer> l_parityNums = new ArrayList<Integer>();
			for (String tok : toks) {
				if (tok.equals("FIX")) {
					isFixer_ = true;
					continue;
				}
				if (tok.equals("ORIGIN")) {
					if (!isFixer_)
						throw new IllegalArgumentException("ORIGIN without FIX");
					recoverOrigin_ = true;
					continue;
				}
				int pn = Integer.parseInt(tok);
				l_parityNums.add(pn);
			}

			isFixer = isFixer_;
			recoverOrigin = recoverOrigin_;
			parityNums = new int[l_parityNums.size()];
			for (int i = 0; i < parityNums.length; i++) {
				parityNums[i] = l_parityNums.get(i);
			} 
		}
	}

	private boolean recoverOrigin(PartInfo part) throws IOException {
		LOG.info("recovering origin for " + part);
		ParityInfo[] existingParities = part.getExistingParities(_util.topFS);
		if (existingParities.length < _util.K) {
			LOG.error(String.format("Unable to recover %s: only %d parities left",
					part.toString(), existingParities.length));
			return false;
		}

		// choose K parities
		OutputStream origin = null;
		InputStream[] parities = new InputStream[_util.K];
		int[] parityNums = new int[_util.K];
		try {
			// XXX use Creator
			//origin = _util.baseFS.create(part.getOriginFile(_util.topFS));
			_parityCreator.addOutputPath(part.getOriginFile(_util.topFS), true);
			origin = _parityCreator.getOriginOutputStream();
			for (int i = 0; i < _util.K; i++) {
				ParityInfo parity = existingParities[i];
				parities[i] = _util.baseFS.open(parity.getParityFile());
				parityNums[i] = parity._parityNum;
				LOG.info("parity " + parity + " is selected");
			}
			_raid.decode(origin, parities, parityNums, part.length);
			LOG.info(String.format("origin for %s recovered",
					part));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (origin != null)
				origin.close();
			for (InputStream p : parities)
				p.close();
		}
		return true;
	}

	// encoding of key:
	// %d %d %d %d...
	// -1 means origin
	// other indicates parity
	public void map(Text key, PartInfo value, Context context)
		throws IOException, InterruptedException
	{
		LOG.info(String.format("map job: key=%s, value=%s",
				key.toString(), value.toString()));
		_util = new NKFSUtil(context.getConfiguration());
		_conf = _util.conf;

		KeyInfo keyinfo = new KeyInfo(key.toString());

		// encode
		_raid = RaidAlgorithm.load(
				RaidAlgorithm.name(_util.raidAlgoName),
				RaidAlgorithm.n(_util.N),
				RaidAlgorithm.k(_util.K)
		);
		
		_parityCreator = ParityCreator.load(
				_util.baseFS,
				value.getPartDir(_util.topFS));


		if (keyinfo.isFixer) {
			value.srcOffset = 0;
			value.srcPath = value.getOriginFile(_util.topFS);
			value.isSrcTopPath = false;
		}

		boolean originCorrect = true;
		if (keyinfo.recoverOrigin) {
			originCorrect = recoverOrigin(value);
		}

		if ((keyinfo.parityNums.length > 0) && (originCorrect)) {
			createParities(keyinfo.parityNums, value);
		}

		/*
		String[] s_ops = key.toString().trim().split(" ");
		if (s_ops[0].equals("FIX")) {
			LOG.info("task for fixer");
			LOG.info("unimpl");
			return;
			// PartInfo should be adjusted: srcPath should have been
			// pointed to correct file origin
		}

		createParities(s_ops, value);
		 */
		context.progress();
	}
}

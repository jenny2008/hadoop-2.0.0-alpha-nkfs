package cn.ict.magicube.math;

import java.io.IOException;

import org.apache.hadoop.util.Options;

import cn.ict.magicube.math.RaidAlgorithm.NameOption;
import cn.ict.magicube.math.RaidAlgorithm.Option;

public class RaidNKProvider extends RaidAlgorithmProvider {

	@Override
	public RaidAlgorithm create(Option... options) {
		try {
			NameOption nameOpt = Options.getOption(NameOption.class, options);
			if (nameOpt == null)
				return null;
			if (!"nk".equals(nameOpt.getValue()))
				return null;
			return new RaidNK(options);
		} catch (IOException e) {
			return null;
		}

	}

}

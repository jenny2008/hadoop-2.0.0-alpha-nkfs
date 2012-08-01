package cn.ict.magicube.fs;

import java.io.IOException;

import org.apache.hadoop.util.Options;

import cn.ict.magicube.fs.ParityCreator.BaseFSOption;
import cn.ict.magicube.fs.ParityCreator.Option;
import cn.ict.magicube.fs.ParityCreator.PartDirOption;

public class CommonParityCreatorProvider extends ParityCreatorProvider {

	@Override
	public ParityCreator create(Option ... options) {
		try {
			BaseFSOption baseFSOpt = Options.getOption(BaseFSOption.class, options);
			PartDirOption partDirOpt = Options.getOption(PartDirOption.class, options);
			if (baseFSOpt == null)
				return null;
			if (baseFSOpt.getValue() == null)
				return null;
			if (partDirOpt == null)
				return null;
			if (partDirOpt.getValue() == null)
				return null;
			return new CommonParityCreator(baseFSOpt.getValue(),
					partDirOpt.getValue());
		} catch (IOException e) {
			return null;
		}
	}
}

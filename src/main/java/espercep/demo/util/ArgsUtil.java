package espercep.demo.util;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/27
 */
public class ArgsUtil {
    private static final Logger logger = LoggerFactory.getLogger(ArgsUtil.class);

    public static CmdLineOptions getArg(String[] args) {
        try {
            CmdLineOptions option = new CmdLineOptions();
            CmdLineParser parser = new CmdLineParser(option);
            parser.parseArgument(args);
            return option;
        } catch (CmdLineException e) {
            logger.error("Error on parse cmd line args: ", e);
            return null;
        }
    }
}

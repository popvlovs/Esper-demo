package espercep.demo.matcher.hyperscan;

import com.hansight.hyperscan.wrapper.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/20
 */
public abstract class HyperscanMatcher {
    private static final Logger logger = LoggerFactory.getLogger(HyperscanMatcher.class);

    protected Set<String> regexps;
    protected Database database;

    protected HyperscanMatcher(Set<String> regexps) {
        this.regexps = regexps;
        long startTime = System.currentTimeMillis();
        this.database = Database.compile(new ArrayList<>(regexps), true);
        logger.info("End of hyperscan build, time elapsed: {}", System.currentTimeMillis() - startTime);
        if (this.database == null) {
            throw new RuntimeException("Compile hyperscan database failed, regexps");
        }
    }

    public abstract boolean match(String value);

    public Set<String> getRegexps() {
        return regexps;
    }

    public void close() {
        if (database == null) {
            return;
        }
        try {
            database.close();
        } catch (IOException e) {
            logger.error("Error on close hyperscan database: ", e);
        }
    }
}

package espercep.demo.matcher.hyperscan;

import java.util.Set;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/20
 */
public class HyperscanPartialMatcher extends HyperscanMatcher {
    private HyperscanPartialMatcher(Set<String> regexps) {
        super(regexps);
    }

    public synchronized boolean match(String value) {
        return this.database.find(value);
    }

    public static HyperscanPartialMatcher compile(Set<String> regexps) {
        return new HyperscanPartialMatcher(regexps);
    }
}

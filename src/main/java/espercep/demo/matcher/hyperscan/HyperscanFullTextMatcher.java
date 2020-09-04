package espercep.demo.matcher.hyperscan;

import java.util.Set;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/20
 */
public class HyperscanFullTextMatcher extends HyperscanMatcher {
    private HyperscanFullTextMatcher(Set<String> regexps) {
        super(regexps);
    }

    @Override
    public synchronized boolean match(String value) {
        return this.database.matches(value);
    }

    public static HyperscanFullTextMatcher compile(Set<String> regexps) {
        return new HyperscanFullTextMatcher(regexps);
    }
}

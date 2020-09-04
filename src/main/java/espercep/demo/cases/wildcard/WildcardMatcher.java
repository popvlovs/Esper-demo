package espercep.demo.cases.wildcard;

import espercep.demo.matcher.HyperscanMatcher;
import espercep.demo.matcher.WildcardTrieMatcher;
import shaded.org.apache.commons.io.FilenameUtils;

import java.util.regex.Pattern;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/6/28
 */
public class WildcardMatcher {
    public static void main(String[] args) {
        String matcher = "*\\icacls.exe";
        String filename = "C://fsadsa/321/dsa/cxa/poivcs//cxaasdas\\dcacls.exe";

        // 1. Commons-io
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            FilenameUtils.wildcardMatch(filename, matcher);
        }
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime) + " ms");
        System.out.println("Result: " + (FilenameUtils.wildcardMatch(filename, matcher)));

        // 2. Wildcard matcher
        WildcardTrieMatcher wildcardTrieMatcher = new WildcardTrieMatcher("C://fsadsa/321/dsa/???/poivcs//*\\dcacls.exe");//, "*\\takeown.exe", "*\\bcacls.exe", matcher);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            wildcardTrieMatcher.match(filename);
        }
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime) + " ms");
        System.out.println("Result: " + (wildcardTrieMatcher.match(filename)));

        // 3. Regexp
        Pattern pattern = Pattern.compile(".*\\\\icacls.exe");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            pattern.matcher(filename).find();
        }
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime) + " ms");
        System.out.println("Result: " + (pattern.matcher(filename).find()));

        // 4. Hyperscan
        HyperscanMatcher hyperscanMatcher = new HyperscanMatcher(".*\\\\icacls.exe");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            hyperscanMatcher.match(filename);
        }
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime) + " ms");
        System.out.println("Result: " + (hyperscanMatcher.match(filename)));
    }
}

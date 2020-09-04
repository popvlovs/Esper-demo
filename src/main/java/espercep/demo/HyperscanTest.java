package espercep.demo;

import espercep.demo.matcher.hyperscan.HyperscanFullTextMatcher;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/8/24
 */
public class HyperscanTest {
    public static void main(String[] args) {
        String[] regexs = new String[]{
                "this\\d+is\\d+text",
                "this\\s+is\\s+text2",
                "this\\s+is\\s+text1",
                "this\\s+is\\s+text",
                "((?:\\d{1}\\.){3}\\d{1})",
                "this\\s+is\\s+text3",
                "^[a-z]+(\\.[0-9]+)",
                "((?:\\d{3}\\.){3}\\d{3})",
                "((?:\\d{2}\\.){3}\\d{2})",
                "^\\d+(\\.\\d+)?",
                "this\\d+is\\s+text",
                "this\\d+is\\s+text2",
                "this\\d+is\\s+text3",
                "^[0-9]+(\\.[a-z]+)",
                "^[a-z]+\\.[a-z]+\\.[a-z]+\\.[0-9]",
                "this\\d+is\\s+text1",
                "^[a-z]+(\\.[a-z]+)",
                "this\\d+is\\d+text3",
                "this\\d+is\\d+text2",
                "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}",
                "this\\d+is\\d+text1",
                "^[a-z]+\\.[a-z]+\\.[a-z]+"
        };
        String value = "11.11.11.11";


        HyperscanFullTextMatcher matcher = HyperscanFullTextMatcher.compile(Arrays.stream(regexs).collect(Collectors.toSet()));
        boolean isMatch = matcher.match(value);
        if (isMatch) {
            System.out.println("value is match");
        } else {
            System.out.println("value not match");
        }
    }
}

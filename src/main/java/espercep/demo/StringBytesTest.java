package espercep.demo;

import java.nio.charset.Charset;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/8
 */
public class StringBytesTest {
    public static void main(String[] args) {
        String text = "aaaa";
        Charset utf8 = Charset.forName("UTF-8");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100_000_000; i++) {
            byte[] bytes = text.getBytes(utf8);
            int len = bytes.length;
            nothing(bytes, len);
        }
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + " ms");
    }

    private static void nothing(byte[] bytes, int len) {

    }
}

package espercep.demo.util;

import com.google.common.hash.*;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;


/**
 * Created by ck on 2017/11/1.
 */

public class MD5Util {
    private static HashFunction hf = Hashing.md5();
    private static Charset defaultCharset = Charset.forName("UTF-8");

    private MD5Util() {}

    public static String md5(String data) {
        HashCode hash = hf.newHasher().putString(data, defaultCharset).hash();
        return hash.toString();
    }

    public static String md5(String data, Charset charset, boolean isUpperCase) {
        HashCode hash = hf.newHasher().putString(data, charset == null ? defaultCharset : charset).hash();
        return isUpperCase ? hash.toString().toUpperCase() : hash.toString();
    }

    public static String md5(byte[] bytes, boolean isUpperCase) {
        HashCode hash = hf.newHasher().putBytes(bytes).hash();
        return isUpperCase ? hash.toString().toUpperCase() : hash.toString();
    }

    public static String md5(File sourceFile, boolean isUpperCase) {
        HashCode hash = hf.newHasher().putObject(sourceFile, new Funnel<File>() {

            private static final long serialVersionUID = 2757585325527511209L;

            @Override
            public void funnel(File from, PrimitiveSink into) {
                try {
                    into.putBytes(Files.toByteArray(from));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).hash();
        return isUpperCase ? hash.toString().toUpperCase() : hash.toString();
    }
}


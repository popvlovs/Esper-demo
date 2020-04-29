package espercep.demo.util;

import org.nustaq.serialization.FSTConfiguration;

import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class FSTUtil {
    static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    public static byte[] serialize(Map data) {
        return conf.asByteArray(data);
    }

    public static Map deserialize(byte[] data) {
        return (Map) conf.asObject(data);
    }
}

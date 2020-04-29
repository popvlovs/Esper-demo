package espercep.demo.util;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class ProtostuffUtil {

    private static Schema<WrapperMap> schema = RuntimeSchema.getSchema(WrapperMap.class);
    private static LinkedBuffer buffer = LinkedBuffer.allocate(512);

    public static byte[] serialize(Map data) {
        try {
            return ProtostuffIOUtil.toByteArray(new WrapperMap(data), schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    public static Map deserialize(byte[] data) {
        WrapperMap deser = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data, deser, schema);
        return deser.getMap();
    }

    private static class WrapperMap {
        private Map<String, Object> map;

        public Map<String, Object> getMap() {
            return map;
        }

        public void setMap(Map<String, Object> map) {
            this.map = map;
        }

        public WrapperMap(Map<String, Object> map) {
            this.map = map;
        }

        public WrapperMap() {
        }
    }
}

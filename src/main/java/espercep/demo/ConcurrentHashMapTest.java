package espercep.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/15
 */
public class ConcurrentHashMapTest {
    public static void main(String[] args) {
        Map<String, String> data = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; ++i) {
            data.put(i + "", i + "");
        }
        Collection<String> values = new ArrayList<>(data.values());
        data.clear();
        values.forEach(System.out::println);
    }
}

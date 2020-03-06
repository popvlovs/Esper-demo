package espercep.demo.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
abstract public class NotBeforeState<T> {

    protected Map<String, T> state;

    public NotBeforeState() {
        this.state = new ConcurrentHashMap<>();
    }

    abstract public T getState(String key);

    abstract public void setState(String key, T val);
}

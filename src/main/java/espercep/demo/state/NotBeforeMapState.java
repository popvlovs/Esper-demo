package espercep.demo.state;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class NotBeforeMapState extends NotBeforeState<Map> {

    /**
     * First active timestamp
     */
    private AtomicLong activeTime;

    public long getActiveTime() {
        return activeTime.get();
    }

    public NotBeforeMapState() {
        super();
        this.activeTime = new AtomicLong(Long.MAX_VALUE);
    }

    @Override
    public Map getState(String key) {
        return this.state.get(key);
    }

    @Override
    public void setState(String key, Map val) {
        this.state.put(key, val);
    }

    public void setMinActiveTime(long activeTime) {
        // Set minimum active time by CAS
        for (;;) {
            long curActiveTime = this.activeTime.get();
            if (activeTime < curActiveTime) {
                if (this.activeTime.compareAndSet(curActiveTime, activeTime)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    public Map getState(Map event, String... groupBy) {
        return this.getState(getDigest(event, groupBy));
    }

    public void setState(Map event, String... groupBy) {
        this.setState(getDigest(event, groupBy), event);
    }

    private String getDigest(Map event, String... groupBy) {
        return Arrays.stream(groupBy)
                .map(field -> event.getOrDefault(field, "null").toString())
                .reduce((a, b) -> a + "#" + b)
                .orElse("Null");
    }
}

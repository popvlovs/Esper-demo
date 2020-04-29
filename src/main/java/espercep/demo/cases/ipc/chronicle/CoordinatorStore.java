package espercep.demo.cases.ipc.chronicle;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class CoordinatorStore {
    private ChronicleMap<String, ConsumerMetaInfo> store;
    private static final long HEARTBEAT_THRESHOLD = TimeUnit.SECONDS.toMillis(60);
    private static final String STORE_FILE_FORMAT = "/dev/shm/ipc-coordinator/{0}";
    private static final Map<String, CoordinatorStore> singleton = new ConcurrentHashMap<>();

    public CoordinatorStore(ChronicleMap store) {
        this.store = store;
    }

    public static CoordinatorStore instance(String topic) {
        try {
            if (!singleton.containsKey(topic)) {
                synchronized (singleton) {
                    if (!singleton.containsKey(topic)) {
                        String storeFilePath = MessageFormat.format(STORE_FILE_FORMAT, topic);
                        File storeFile = new File(storeFilePath);
                        if (!storeFile.getParentFile().exists()) {
                            storeFile.getParentFile().mkdirs();
                        }
                        ChronicleMap<String, ConsumerMetaInfo> store = ChronicleMapBuilder.of(String.class, ConsumerMetaInfo.class)
                                .name("ipc-coordinator")
                                .averageKey("master_slave")
                                .averageValue(new ConsumerMetaInfo())
                                .entries(1000)
                                .createPersistedTo(storeFile);
                        singleton.put(topic, new CoordinatorStore(store));
                    }
                }
            }
            return singleton.get(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String key, ConsumerMetaInfo val) {
        store.put(key, val);
    }

    public ConsumerMetaInfo get(String key) {
        return store.get(key);
    }

    public ConsumerMetaInfo getOrElse(String key, Supplier<ConsumerMetaInfo> elseGet) {
        return Optional.ofNullable(get(key)).orElseGet(elseGet);
    }

    public boolean delete(String key) {
        ConsumerMetaInfo del = store.remove(key);
        return del != null;
    }

    public Collection<ConsumerMetaInfo> availableConsumers() {
        long now = System.currentTimeMillis();
        return store.values().stream()
                .filter(consumer -> (now - consumer.getHeartbeat() < HEARTBEAT_THRESHOLD))
                .collect(Collectors.toSet());
    }

    public ConsumerMetaInfo getReleasedStoreFileBarrier() {
        return this.availableConsumers().stream()
                .filter(consumer -> consumer.getReleasedStoreFile() != null)
                .min(Comparator.comparing(ConsumerMetaInfo::getReleasedStoreFile))
                .orElse(null);
    }

    public Collection<ConsumerMetaInfo> getAllConsumers() {
        return store.values();
    }

    public void register(ConsumerMetaInfo consumer) {
        this.put(consumer.getId(), consumer);
    }

    public void registerOnStartup(String consumerId) {
        ConsumerMetaInfo consumer = Optional.ofNullable(this.get(consumerId)).orElseGet(ConsumerMetaInfo::new);
        consumer.setId(consumerId);
        consumer.setHeartbeat(System.currentTimeMillis());
    }
}

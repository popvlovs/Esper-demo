package espercep.demo.cases.ipc.chronicle;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/8
 */
public class ChronicleMessageChannelBuilders<T> {
    private String topic;
    private Class<T> clazz;

    public static <T> ChronicleMessageChannelBuilders<T> builder(Class<T> clazz) {
        return new ChronicleMessageChannelBuilders<>();
    }

    public ChronicleMessageChannelBuilders<T> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public ChronicleMessageChannel<T> buildConsumer(String id) {
        return new ChronicleMessageChannel<>(id, topic, ChronicleMessageRole.CONSUMER, clazz);
    }

    public ChronicleMessageChannel<T> buildProducer(String id) {
        return new ChronicleMessageChannel<>(id, topic, ChronicleMessageRole.PRODUCER, clazz);
    }
}

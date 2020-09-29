package espercep;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.flatbuffers.FlexBuffers;
import com.google.flatbuffers.FlexBuffersBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/9/29
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 1)
@State(value = Scope.Benchmark)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FlatBufferBenchmark {

    private byte[] bytes;
    private FlexBuffers.Map map;
    private JSONObject json;
    private String jsonText;

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder().include(FlatBufferBenchmark.class.getSimpleName())
                .forks(2)
                .build();
        new Runner(options).run();
    }

    @Setup
    public void init() {
        bytes = readByteFromFile("C:\\projects\\esper6-cep-demo\\data.binary");
        map = FlexBuffers.getRoot(ByteBuffer.wrap(bytes)).asMap();
        jsonText = "{\"original_log\":\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"event_id\":1024,\"occur_time\":1601361027750,\"src_address\":\"172.16.101.1\",\"dst_address\":\"172.16.101.0\",\"event_name\":\"windows网络连接\"}";
        json = JSON.parseObject(jsonText);
    }

    @Fork(2)
    @Benchmark
    public void flatbuffersGetBuffer(Blackhole blackhole) {
        FlexBuffers.Map buffer = FlexBuffers.getRoot(ByteBuffer.wrap(bytes)).asMap();
        blackhole.consume(buffer);
    }

    @Fork(2)
    @Benchmark
    public void flatbuffersReadString(Blackhole blackhole) {
        String srcAddress = map.get("src_address").asString();
        blackhole.consume(srcAddress);
    }

    @Fork(2)
    @Benchmark
    public void flatbuffersReadLongString(Blackhole blackhole) {
        String originalLog = map.get("original_log").asString();
        blackhole.consume(originalLog);
    }

    @Fork(2)
    @Benchmark
    public void flatbuffersReadLong(Blackhole blackhole) {
        long eventId = map.get("event_id").asLong();
        blackhole.consume(eventId);
    }

    @Fork(2)
    @Benchmark
    public void flatbuffersWrite(Blackhole blackhole) {
        FlexBuffersBuilder buffersBuilder = new FlexBuffersBuilder(ByteBuffer.allocate(2048),
                FlexBuffersBuilder.BUILDER_FLAG_SHARE_KEYS_AND_STRINGS);

        // Write
        int dataStart = buffersBuilder.startMap();
        buffersBuilder.putInt("event_id", 1024);
        buffersBuilder.putString("event_name", "windows网络连接");
        buffersBuilder.putString("src_address", "172.16.101.1");
        buffersBuilder.putString("dst_address", "172.16.101.0");
        buffersBuilder.putString("original_log", Strings.repeat("d", 1024));
        buffersBuilder.putInt("occur_time", System.currentTimeMillis());
        buffersBuilder.endMap(null, dataStart);
        ByteBuffer byteBuffer = buffersBuilder.finish();
        blackhole.consume(byteBuffer);
    }

    @Fork(2)
    @Benchmark
    public void jsonDeserialize(Blackhole blackhole) {
        JSONObject jsonObj = JSON.parseObject(jsonText);
        blackhole.consume(jsonObj);
    }

    @Fork(2)
    @Benchmark
    public void jsonSerialize(Blackhole blackhole) {
        String text = json.toJSONString();
        blackhole.consume(text);
    }

    @Fork(2)
    @Benchmark
    public void jsonReadString(Blackhole blackhole) {
        String srcAddress = json.getString("src_address");
        blackhole.consume(srcAddress);
    }

    @Fork(2)
    @Benchmark
    public void jsonReadLongString(Blackhole blackhole) {
        String originalLog = json.getString("original_log");
        blackhole.consume(originalLog);
    }

    @Fork(2)
    @Benchmark
    public void jsonReadLong(Blackhole blackhole) {
        long eventId = json.getLong("event_id");
        blackhole.consume(eventId);
    }

    private static byte[] readByteFromFile(String absoluteFilePath) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        File storeFile = new File(absoluteFilePath);

        byte[] buffer = new byte[512];
        try (InputStream stream = new FileInputStream(storeFile)) {
            int len;
            while ((len = stream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputStream.toByteArray();
    }
}

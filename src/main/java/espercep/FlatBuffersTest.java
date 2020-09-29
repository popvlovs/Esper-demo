package espercep;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.flatbuffers.ArrayReadWriteBuf;
import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.FlexBuffers;
import com.google.flatbuffers.FlexBuffersBuilder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/9/29
 */
public class FlatBuffersTest {
    public static void main(String[] args) {
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

        writeToFile("C:\\projects\\esper6-cep-demo\\data.binary", byteBuffer);

        // Read
        byte[] readBytes = readByteFromFile("C:\\projects\\esper6-cep-demo\\data.binary");
        long startTime = System.currentTimeMillis();
        int count = 0;
        for (int i = 0; i < 1_000_000; ++i) {
            FlexBuffers.Map map = FlexBuffers.getRoot(ByteBuffer.wrap(readBytes)).asMap();
            if (map.get("event_id").asLong() > 0L) {
                count++;
            }
        }
        System.out.println("Time elapsed for read: " + (System.currentTimeMillis() - startTime) + " ms, count: " + count);
        // System.out.println("Read original log as int: " + map.get("event_id").asLong());
        // System.out.println("Read from byte buffer: " + map.toString());
    }

    private static void writeToFile(String absoluteFilePath, ByteBuffer byteBuffer) {
        File storeFile = new File(absoluteFilePath);
        if (!storeFile.getParentFile().exists()) {
            storeFile.getParentFile().mkdirs();
        }
        try {
            if (!storeFile.exists()) {
                if (!storeFile.createNewFile()) {
                    System.out.println("Cannot create file: " + storeFile.getCanonicalPath());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileChannel fc = new FileOutputStream(storeFile).getChannel()) {
            fc.write(byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

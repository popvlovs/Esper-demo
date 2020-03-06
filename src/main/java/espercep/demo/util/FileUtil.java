package espercep.demo.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class FileUtil {
    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static String readResourceAsString(String resourceName) throws IOException {
        resourceName = "epl/" + resourceName;
        ClassLoader classLoader = FileUtil.class.getClassLoader();
        return readToString(classLoader.getResourceAsStream(resourceName));
    }

    public static String readToString(InputStream stream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        String encoding = "UTF-8";
        byte[] buffer = new byte[1024];

        int len;
        while ((len = stream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, len);
        }
        try {
            return outputStream.toString(encoding);
        } catch (UnsupportedEncodingException e) {
            logger.error("不支持的编码格式：", encoding);
        }
        return null;
    }

    public static String readJarFileToString(String fileName) {
        try {
            ClassLoader classLoader = FileUtil.class.getClassLoader();
            String basePackage = FileUtil.class.getPackage().getName();
            URL resource = classLoader.getResource(basePackage.replaceAll("\\.", "/"));
            JarURLConnection connection = (JarURLConnection) resource.openConnection();
            JarFile jar = connection.getJarFile();
            JarEntry entry = jar.getJarEntry(fileName);
            InputStream stream = jar.getInputStream(entry);
            return FileUtil.readToString(stream);
        } catch (IOException e) {
            throw new RuntimeException("read jar file error: " + fileName, e);
        }
    }

    public static boolean streamToFile(InputStream is, String file) {
        try (FileOutputStream fo = new FileOutputStream(file, true)) {
            int len;
            byte[] buf = new byte[1024];
            while ((len = is.read(buf)) > 0) {
                fo.write(buf, 0, len);
            }
            return true;
        } catch (Exception e) {
            logger.error("write file failed: ", e);
            return false;
        }
    }
}

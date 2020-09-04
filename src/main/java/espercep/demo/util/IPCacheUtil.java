package espercep.demo.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Description: ip cache
 *
 * @Date: 2020/6/5
 */
public class IPCacheUtil {
    public static long ipv4ToLong(String host) {
        try {
            InetAddress ipv4 = InetAddress.getByName(host);
            // signed int -> unsigned long
            return ByteBuffer.wrap(ipv4.getAddress()).getInt() & 0xFFFFFFFFL;
        } catch (UnknownHostException e) {
            return -1L;
        }
    }
}

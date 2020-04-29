package espercep.demo.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

/**
 * Description: ip function utilities
 *
 * @Date: 2018/6/21
 */
public class IpUtil {
    public static long ipv4ToLong(String host) {
        try {
            InetAddress ipv4 = InetAddress.getByName(host);
            // signed int -> unsigned long
            return ByteBuffer.wrap(ipv4.getAddress()).getInt() & 0xFFFFFFFFL;
        } catch (UnknownHostException e) {
            return -1L;
        }
    }

    /**
     * Description: change ip string to long
     *
     * @Date: 2018/6/21
     */
    public static long getIp2Long(String ip) {
        long ip2long = 0L;
        try {
            ip = ip.trim();
            StringTokenizer stringTokenizer = new StringTokenizer(ip, ".");
            String[] ips = new String[4];
            int k = 0;
            while (stringTokenizer.hasMoreElements()){
                ips[k++] = stringTokenizer.nextToken();
                if(k >=4) break;
            }
            //非IP数据判断
            if(k < 4){
                return -1;
            }
            for (int i = 0; i < 4; ++i) {
                ip2long = ip2long << 8 | Integer.parseInt(ips[i]);
            }
        } catch (Exception e) {
            return -1;
        }
        return ip2long;
    }
}

package espercep.demo.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.regex.Pattern;

/**
 * Description: ip function utilities
 *
 * @Date: 2018/6/21
 */
public class IpUtil {
    private static Pattern pattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");


    /**
     * 私有IP：A类  10.0.0.0-10.255.255.255
     * B类  172.16.0.0-172.31.255.255
     * C类  192.168.0.0-192.168.255.255
     * 当然，还有127这个网段是环回地址
     **/
    private static long aBegin, aEnd, bBegin, bEnd, cBegin, cEnd, localhost, broadcast, router;

    static {
        aBegin = getIp2Long("10.0.0.0");
        aEnd = getIp2Long("10.255.255.255");
        bBegin = getIp2Long("172.16.0.0");
        bEnd = getIp2Long("172.31.255.255");
        cBegin = getIp2Long("192.168.0.0");
        cEnd = getIp2Long("192.168.255.255");
        localhost = getIp2Long("127.0.0.1");
        router = getIp2Long("0.0.0.0");
        broadcast = getIp2Long("255.255.255.255");
    }

    public static boolean isTraditionalIntrenat(String ip) {
        return isIntrenat(getIp2Long(ip));
    }
    public static boolean isIntrenat(long ip) {
        return (aBegin <= ip && ip <= aEnd) ||
                (bBegin <= ip && ip <= bEnd) ||
                (cBegin <= ip && ip <= cEnd) ||
                ip == localhost || ip == router || ip == broadcast;
    }

    /**
     * Description: check if string is ip
     *
     * @Date: 2018/6/21
     */
    public static boolean isIP(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        return pattern.matcher(str).matches();
    }

    /**
     * Description: change ip string to long
     *
     * @Date: 2018/6/21
     */
    public static long getIp2Long(String host) {
        return IPCacheUtil.ipv4ToLong(host);
    }

    public static ImmutablePair<Long, Long> getCIDR2LongPair(String ipv4, String mask) {
        try {
            return getCIDR2LongPair(ipv4, Integer.parseInt(mask));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static ImmutablePair<Long, Long> getCIDR2LongPair(String ipv4, int mask) {
        long value = getIp2Long(ipv4);
        if (value < 0) {
            return null;
        }
        if (mask > 32 || mask <= 0) {
            return null;
        }
        mask = 0xFFFFFFFF << (32 - mask);
        long start = value & mask;
        long end = value | ~mask;
        return new ImmutablePair<>(start, end);
    }
}

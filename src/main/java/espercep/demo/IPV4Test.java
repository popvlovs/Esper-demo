package espercep.demo;

import espercep.demo.util.IpUtil;
import espercep.demo.util.OldIpUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/6/12
 */
public class IPV4Test {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; ++i) {
            OldIpUtil.NetMask mask = OldIpUtil.getNetFromMask("126.1.0.0", 15);
        }
        System.out.println("Old CIDR to long elapsed: " + (System.currentTimeMillis() - start) + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < 100000; ++i) {
            ImmutablePair<Long, Long> pair = IpUtil.getCIDR2LongPair("126.1.0.0", "15");
        }
        System.out.println("New CIDR to long elapsed: " + (System.currentTimeMillis() - start) + " ms");
    }
}

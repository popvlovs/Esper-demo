package espercep.demo.cases;

import espercep.demo.util.IpUtil;

import java.text.MessageFormat;
import java.util.Random;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class ipv4_2_long_1 {
    public static void main(String[] args) throws Exception {
        int loop = 1000000;
        String[] ips = new String[loop];
        Random random = new Random();
        for (int i = 0; i < loop; i++) {
            ips[i] = MessageFormat.format( "172.16.{0}.{1}", random.nextInt(255), random.nextInt(255));
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            IpUtil.getIp2Long(ips[i]);
        }
        System.out.println("IpUtil.getIp2Long cost: " + (System.currentTimeMillis() - start) + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            IpUtil.ipv4ToLong(ips[i]);
        }
        System.out.println("IpUtil.ipv4ToLong cost: " + (System.currentTimeMillis() - start) + " ms");

    }

}

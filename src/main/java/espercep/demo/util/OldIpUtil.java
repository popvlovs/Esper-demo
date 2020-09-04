package espercep.demo.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/6/15
 */
public class OldIpUtil {
    public static NetMask getNetFromMask(String ip, int mask) {
        if (StringUtils.isBlank(ip))
            return null;
        String maskStr = getMask(mask);
        if (StringUtils.isBlank(maskStr))
            return null;
        return getNetFromMask(ip, maskStr);
    }

    public static NetMask getNetFromMask(String ip, String mask) {
        if (StringUtils.isBlank(ip) || StringUtils.isBlank(mask))
            return null;
        NetMask nets = new NetMask();
        String[] start = Negation(ip, mask).split("\\.");
        nets.setStartIP(start[0] + "." + start[1] + "." + start[2] + "." + (Integer.valueOf(start[3]) + 1));
        nets.setEndIP(TaskOR(Negation(ip, mask), mask));
        nets.setMask(mask);
        return nets;
    }

    /**
     * Description: get mask ip from maskbits
     *
     * @Date: 2018/6/21
     */
    private static String getMask(int masks) {
        if (masks == 1) return "128.0.0.0";
        if (masks == 2) return "192.0.0.0";
        if (masks == 3) return "224.0.0.0";
        if (masks == 4) return "240.0.0.0";
        if (masks == 5) return "248.0.0.0";
        if (masks == 6) return "252.0.0.0";
        if (masks == 7) return "254.0.0.0";
        if (masks == 8) return "255.0.0.0";
        if (masks == 9) return "255.128.0.0";
        if (masks == 10) return "255.192.0.0";
        if (masks == 11) return "255.224.0.0";
        if (masks == 12) return "255.240.0.0";
        if (masks == 13) return "255.248.0.0";
        if (masks == 14) return "255.252.0.0";
        if (masks == 15) return "255.254.0.0";
        if (masks == 16) return "255.255.0.0";
        if (masks == 17) return "255.255.128.0";
        if (masks == 18) return "255.255.192.0";
        if (masks == 19) return "255.255.224.0";
        if (masks == 20) return "255.255.240.0";
        if (masks == 21) return "255.255.248.0";
        if (masks == 22) return "255.255.252.0";
        if (masks == 23) return "255.255.254.0";
        if (masks == 24) return "255.255.255.0";
        if (masks == 25) return "255.255.255.128";
        if (masks == 26) return "255.255.255.192";
        if (masks == 27) return "255.255.255.224";
        if (masks == 28) return "255.255.255.240";
        if (masks == 29) return "255.255.255.248";
        if (masks == 30) return "255.255.255.252";
        if (masks == 31) return "255.255.255.254";
        if (masks == 32) return "255.255.255.255";
        return "";
    }

    /**
     * Description: calculate start ip from mask
     *
     * @Date: 2018/6/21
     */
    private static String Negation(String StartIP, String netmask) {
        String[] temp1 = StartIP.trim().split("\\.");
        String[] temp2 = netmask.trim().split("\\.");
        int[] rets = new int[4];
        for (int i = 0; i < 4; i++) {
            rets[i] = Integer.parseInt(temp1[i]) & Integer.parseInt(temp2[i]);
        }
        return rets[0] + "." + rets[1] + "." + rets[2] + "." + rets[3];
    }

    /**
     * Description: calculate end ip from mask
     *
     * @Date: 2018/6/21
     */
    private static String TaskOR(String StartIP, String netmask) {
        String[] temp1 = StartIP.trim().split("\\.");
        String[] temp2 = netmask.trim().split("\\.");
        int[] rets = new int[4];
        for (int i = 0; i < 4; i++) {
            rets[i] = 255 - (Integer.parseInt(temp1[i]) ^ Integer.parseInt(temp2[i]));
        }
        return rets[0] + "." + rets[1] + "." + rets[2] + "." + (rets[3] - 1);
    }

    /**
     * Description: public class for mask net
     *
     * @Date: 2018/6/21
     */
    public static class NetMask {
        private String startIP;
        private String endIP;
        private String mask;

        public String getStartIP() {
            return startIP;
        }

        public void setStartIP(String startIP) {
            this.startIP = startIP;
        }

        public String getEndIP() {
            return endIP;
        }

        public void setEndIP(String endIP) {
            this.endIP = endIP;
        }

        public String getMask() {
            return mask;
        }

        public void setMask(String mask) {
            this.mask = mask;
        }

        @Override
        public String toString() {
            return "NetMask{" +
                    "startIP='" + startIP + '\'' +
                    ", endIP='" + endIP + '\'' +
                    ", mask='" + mask + '\'' +
                    '}';
        }
    }
}

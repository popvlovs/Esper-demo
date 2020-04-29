package espercep.demo.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.kohsuke.args4j.Option;

import javax.annotation.Nonnull;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/27
 */
public class CmdLineOptions {
    @Option(name = "-ruleNum", aliases = {"-rule"}, usage = "运行的规则数")
    private Integer ruleNum;

    @Option(name = "-coreNum", aliases = {"-cpu"}, usage = "使用的核心数")
    private Integer coreNum;

    @Option(name = "-groupByNum", aliases = {"-group"}, usage = "分组字段数")
    private Integer groupByNum;

    @Option(name = "-groupByComplexity", aliases = {"-complexity"}, usage = "每个分组中的复杂度")
    private Integer groupByComplexity;

    @Option(name = "-disruptorWaitingStrategy", aliases = {"-waiting"}, usage = "线程等待策略")
    private String waitingStrategy;

    @Option(name = "-ringBufferSize", aliases = {"-buffer"}, usage = "ringBuffer长度")
    private Integer ringBufferSize;

    @Option(name = "-disruptorOnly", aliases = {"-no-esper"}, usage = "禁用esper，只测试disruptor的性能")
    private boolean noEsper;

    @Option(name = "-cpuAffinity", aliases = {"-affinity"}, usage = "开启CPU Affinity设置")
    private boolean cpuAffinity;

    @Option(name = "-generateOnThread", aliases = {"-no-disruptor"}, usage = "在每个线程中独立生成，不启用disruptor")
    private boolean noDisruptor;

    @Option(name = "-disableMetric", aliases = {"-no-metric"}, usage = "不进行metric统计")
    private boolean noMetric;

    @Option(name = "-availableCores", aliases = {"-cores"}, usage = "允许使用的cpu核心")
    private String availableCores;

    @Option(name = "-eventNum", aliases = {"-event"}, usage = "测试的事件数量")
    private Long eventNum;

    @Option(name = "-chronicle-consumer-id", aliases = {"-chronicle"}, usage = "Chronicle消费者进程编号")
    private String chronicleId;

    @Override
    public String toString() {
        return JSONObject.toJSONString(this, true);
    }

    public int getRuleNum() {
        return Optional.ofNullable(ruleNum).orElse(48);
    }

    public void setRuleNum(Integer ruleNum) {
        this.ruleNum = ruleNum;
    }

    public int getCoreNum() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        return Optional.ofNullable(coreNum).orElse(Integer.min(24, cpuCores));
    }

    public void setCoreNum(Integer coreNum) {
        this.coreNum = coreNum;
    }

    public int getGroupByNum() {
        return Optional.ofNullable(groupByNum).orElse(3);
    }

    public void setGroupByNum(Integer groupByNum) {
        this.groupByNum = groupByNum;
    }

    public int getGroupByComplexity() {
        return Optional.ofNullable(groupByComplexity).orElse(16);
    }

    public void setGroupByComplexity(Integer groupByComplexity) {
        this.groupByComplexity = groupByComplexity;
    }

    public String getWaitingStrategyClass() {
        return Optional.ofNullable(waitingStrategy).orElse(YieldingWaitStrategy.class.getCanonicalName());
    }

    @JSONField(serialize = false)
    public WaitStrategy getWaitingStrategy() {
        String classname = getWaitingStrategyClass();
        try {
            return (WaitStrategy) Class.forName(classname).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @JSONField(serialize = false)
    public ThreadFactory getThreadFactory(int[] availableCores) {
        if (cpuAffinity) {
            return new AffinityThreadFactory(availableCores);
        } else {
            return Executors.defaultThreadFactory();
        }
    }

    @JSONField(serialize = false)
    public ThreadFactory getThreadFactory() {
        if (cpuAffinity) {
            return new AffinityThreadFactory();
        } else {
            return Executors.defaultThreadFactory();
        }
    }

    public void setWaitingStrategy(String waitingStrategy) {
        this.waitingStrategy = waitingStrategy;
    }

    public int getRingBufferSize() {
        return Optional.ofNullable(ringBufferSize).orElse(2 << 13);
    }

    public void setRingBufferSize(Integer ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
    }

    public boolean isCpuAffinity() {
        return cpuAffinity;
    }

    public void setCpuAffinity(boolean cpuAffinity) {
        this.cpuAffinity = cpuAffinity;
    }

    public boolean isNoEsper() {
        return noEsper;
    }

    public void setNoEsper(boolean noEsper) {
        this.noEsper = noEsper;
    }

    public boolean isNoDisruptor() {
        return noDisruptor;
    }

    public void setNoDisruptor(boolean noDisruptor) {
        this.noDisruptor = noDisruptor;
    }

    public boolean isNoMetric() {
        return noMetric;
    }

    public void setNoMetric(boolean noMetric) {
        this.noMetric = noMetric;
    }

    public int[] getAvailableCores() {
        if (this.availableCores == null) {
            return new int[0];
        }
        return Stream.of(this.availableCores.split(","))
                .filter(Objects::nonNull)
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    public void setAvailableCores(String availableCores) {
        this.availableCores = availableCores;
    }

    public long getEventNum() {
        return Optional.ofNullable(eventNum).orElse(Long.MAX_VALUE);
    }

    public void setEventNum(Long eventNum) {
        this.eventNum = eventNum;
    }

    private static String RANDOM_CHRONICLE_ID;
    static {
            try {
                RANDOM_CHRONICLE_ID = MD5Util.md5(Long.toString(System.currentTimeMillis()));
            } catch (Exception e) {
                RANDOM_CHRONICLE_ID = "FixedChronicleNum";
            }
    }

    public String getChronicleId() {
        return Optional.ofNullable(chronicleId).orElse(RANDOM_CHRONICLE_ID);
    }

    public void setChronicleId(String chronicleId) {
        this.chronicleId = chronicleId;
    }
}

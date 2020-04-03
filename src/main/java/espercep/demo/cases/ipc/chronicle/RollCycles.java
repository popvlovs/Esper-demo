package espercep.demo.cases.ipc.chronicle;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.RollCycle;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/3
 */
public enum RollCycles implements RollCycle {
    SECONDLY_5("yyyyMMdd-HHmmss'T'", 5000, 1024, 16),
    SECONDLY_1("yyyyMMdd-HHmmss'T'", 1000, 1024, 16);

    private static final Iterable<RollCycles> VALUES = Arrays.asList(values());
    private final String format;
    private final int length;
    private final int cycleShift;
    private final int indexCount;
    private final int indexSpacing;
    private final long sequenceMask;

    public static Iterable<RollCycles> all() {
        return VALUES;
    }

    private RollCycles(String format, int length, int indexCount, int indexSpacing) {
        this.format = format;
        this.length = length;
        this.indexCount = Maths.nextPower2(indexCount, 8);
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        this.cycleShift = Math.max(32, Maths.intLog2((long)indexCount) * 2 + Maths.intLog2((long)indexSpacing));
        this.sequenceMask = (1L << this.cycleShift) - 1L;
    }

    public String format() {
        return this.format;
    }

    public int length() {
        return this.length;
    }

    public int defaultIndexCount() {
        return this.indexCount;
    }

    public int defaultIndexSpacing() {
        return this.indexSpacing;
    }

    public int current(@NotNull TimeProvider time, long epoch) {
        return (int)((time.currentTimeMillis() - epoch) / (long)this.length());
    }

    public long toIndex(int cycle, long sequenceNumber) {
        return ((long)cycle << this.cycleShift) + (sequenceNumber & this.sequenceMask);
    }

    public long toSequenceNumber(long index) {
        return index & this.sequenceMask;
    }

    public int toCycle(long index) {
        return Maths.toUInt31(index >> this.cycleShift);
    }
}

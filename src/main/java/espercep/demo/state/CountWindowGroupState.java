package espercep.demo.state;

import java.util.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/24
 */
public class CountWindowGroupState<E extends Map> {

    private KeyExtractor keyExtractor;
    private final Map<String, CountWindowState<E>> groupedState = new HashMap<>();
    private long startTime = Long.MAX_VALUE;
    private long endTime = 0L;
    private Map<String, String> outputWindowFields;
    private Map<String, String> outputLastFields;
    private String outputCountAs;
    private Map.Entry<String, String> outputSumFields;
    private String timeFiled;

    public CountWindowGroupState() {
        this.keyExtractor = new KeyExtractor(new String[0]);
        this.outputWindowFields = new HashMap<>();
        this.outputLastFields = new HashMap<>();
        this.outputSumFields = null;
        this.timeFiled = "occur_time";
    }

    public CountWindowGroupState<E> groupBy(String... groupByKeys) {
        this.keyExtractor = new KeyExtractor(groupByKeys);
        return this;
    }

    public CountWindowGroupState<E> outputWindowAs(String field, String asField) {
        this.outputWindowFields.put(field, asField);
        return this;
    }

    public CountWindowGroupState<E> outputCountAs(String asField) {
        this.outputCountAs = asField;
        return this;
    }

    public CountWindowGroupState<E> outputSumAs(String field, String asField) {
        this.outputSumFields = new AbstractMap.SimpleImmutableEntry<>(field, asField);
        return this;
    }

    public CountWindowGroupState<E> timeField(String timeFiled) {
        this.timeFiled = timeFiled;
        return this;
    }

    public CountWindowGroupState<E> outputLastAs(String field, String asField) {
        this.outputLastFields.put(field, asField);
        return this;
    }

    public List<Map<String, Object>> applyEntry(E element) {
        // Extract groupKey & occurTime
        String key = keyExtractor.extract(element);
        Object occurTimeObj = element.get(this.timeFiled);
        if (Objects.isNull(occurTimeObj)) {
            return null;
        }
        long occurTime = (long) occurTimeObj;

        // If should output
        List<Map<String, Object>> output = null;
        if (startTime != Long.MAX_VALUE && occurTime - startTime > 1000L) {
            output = rotate();
        }

        // Get state of groupKey and apply entry
        CountWindowState<E> state;
        if (!groupedState.containsKey(key)) {
            state = new CountWindowState<>(outputCountAs, outputWindowFields, outputLastFields, outputSumFields);
            groupedState.put(key, state);
        } else {
            state = groupedState.get(key);
        }

        startTime = Long.min(occurTime, startTime);
        endTime = Long.max(occurTime, endTime);
        state.applyEntry(element, (long) occurTimeObj);
        return output;
    }

    private List<Map<String, Object>> rotate() {
        List<Map<String, Object>> output = new ArrayList<>();
        for (Map.Entry<String, CountWindowState<E>> entry : groupedState.entrySet()) {
            output.add(entry.getValue().rotate());
        }
        groupedState.clear();
        startTime = Long.MAX_VALUE;
        endTime = 0L;
        return output;
    }

    public class KeyExtractor {
        private String[] groupByKeys;

        public KeyExtractor(String[] groupByKeys) {
            this.groupByKeys = groupByKeys;
        }

        public String extract(E element) {
            StringBuilder sb = new StringBuilder();
            for (String key : groupByKeys) {
                Object val = element.get(key);
                if (val != null) {
                    sb.append(val.toString()).append(";");
                } else {
                    sb.append("NULL").append(";");
                }
            }
            return sb.toString();
        }
    }
}

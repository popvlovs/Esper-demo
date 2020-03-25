package espercep.demo.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/24
 */
public class CountWindowState<E extends Map> {

    private long startTime;
    private long endTime;
    private long count;
    private String outputCountAs;
    private Map<String, String> outputWindowFields;
    private Map<String, String> outputLastFields;
    private Map.Entry<String, String> outputSumFields;
    private E lastElement;
    private E firstElement;
    private List<E> window;

    public CountWindowState(String outputCountAs,
                            Map<String, String> outputWindowFields,
                            Map<String, String> outputLastFields,
                            Map.Entry<String, String> outputSumFields) {
        this.outputCountAs = outputCountAs;
        this.outputLastFields = outputLastFields;
        this.outputWindowFields = outputWindowFields;
        this.outputSumFields = outputSumFields;
        this.window = new ArrayList<>();
        reset();
    }

    public void applyEntry(E element, long occurTime) {
        if (occurTime < this.startTime) {
            this.startTime = occurTime;
            this.firstElement = element;
        }
        if (occurTime >= this.endTime) {
            this.endTime = occurTime;
            this.lastElement = element;
        }
        this.count++;
        this.window.add(element);
    }

    public Map<String, Object> rotate() {
        Map<String, Object> result = new HashMap<>();
        result.put("start_time", this.startTime);
        result.put("end_time", this.endTime);
        result.put(this.outputCountAs, this.count);
        for (Map.Entry<String, String> outputWindowField : this.outputWindowFields.entrySet()) {
            List<Object> values = new ArrayList<>();
            String field = outputWindowField.getKey();
            for (E element : window) {
                values.add(element.get(field));
            }
            result.put(outputWindowField.getValue(), values);
        }
        if (lastElement != null) {
            for (Map.Entry<String, String> outputLastField : this.outputLastFields.entrySet()) {
                result.put(outputLastField.getValue(), lastElement.get(outputLastField.getKey()));
            }
        }
        if (this.outputSumFields != null) {
            long sum = 0L;
            for (E element : window) {
                sum += (long) element.get(this.outputSumFields.getKey());
            }
            result.put(this.outputSumFields.getValue(), sum);
        }
        reset();
        return result;
    }

    private void reset() {
        this.startTime = Long.MAX_VALUE;
        this.endTime = 0L;
        this.count = 0L;
        this.firstElement = null;
        this.lastElement = null;
        this.window.clear();
    }
}

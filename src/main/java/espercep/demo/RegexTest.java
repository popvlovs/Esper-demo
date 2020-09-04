package espercep.demo;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/17
 */
public class RegexTest {

    private static final Pattern CONDITION_PARAM_REGEX = Pattern.compile("\\$\\{(\\w+)}");

    public static void main(String[] args) {
        String condition = "设备地址=\"${src_address1}\" and 目的地址=\"${src_addres}\" and ${dst_address}";
        ConditionFormatter conditionFormatter = ConditionFormatter.build(condition);
        Map<String, Object> data = new HashMap<>();
        data.put("src_address", "123");
        data.put("dst_address", "234");
        System.out.println(conditionFormatter.format(data));
    }

    private static class ConditionFormatter {
        private static final Pattern REGEX = Pattern.compile("\\$\\{(\\w+)}");

        private String condition;
        private Object[] slices;
        private String[] fields;

        private ConditionFormatter(String condition, Object[] slices, String[] fields) {
            this.condition = condition;
            this.slices = slices;
            this.fields = fields;
        }

        public static ConditionFormatter build(String condition) {
            Matcher matcher = REGEX.matcher(condition);
            List<Object> slices = new ArrayList<>();
            List<String> fields = new ArrayList<>();

            int pos = 0;
            while (matcher.find()) {
                String field = matcher.group();
                if (field.length() < 4) {
                    continue;
                }
                field = field.substring(2, field.length()-1);
                int fieldIdx = fields.indexOf(field);
                if (fieldIdx < 0) {
                    fields.add(field);
                    fieldIdx = fields.size() - 1;
                }
                int startPos = matcher.start();
                slices.add(condition.substring(pos, startPos));
                slices.add(fieldIdx);
                pos = matcher.end();
            }
            if (pos < condition.length()) {
                slices.add(condition.substring(pos));
            }
            return new ConditionFormatter(condition, slices.toArray(new Object[0]), fields.toArray(new String[0]));
        }

        public String format(Map<String, Object> data) {
            String[] values = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                String value = Optional.ofNullable(data.get(field)).orElse("Null").toString();
                values[i] = value;
            }
            StringBuilder sb = new StringBuilder();
            for (Object slice : slices) {
                if (slice instanceof String) {
                    sb.append(slice);
                } else if (slice instanceof Integer) {
                    sb.append(values[(int) slice]);
                } else {
                    throw new IllegalArgumentException("Unexpected slice class type: " + slice.getClass().getCanonicalName());
                }
            }
            return sb.toString();
        }
    }
}

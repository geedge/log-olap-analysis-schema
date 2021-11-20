package com.zdjizhi.utils.functions;

import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.general.MetricFunctions;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/7/2113:55
 */
public class FirstCountWindowFunction extends ProcessWindowFunction<Tuple4<String, String, String, String>, Tuple2<String, String>, String, TimeWindow> {
    private static final Logger logger = LoggerFactory.getLogger(FirstCountWindowFunction.class);

    private static HashMap<String, String[]> metricsMap = JsonParseUtil.getMetricsMap();
    private static HashMap<String, String[]> actionMap = JsonParseUtil.getActionMap();
    private HashMap<String, Map<String, Object>> cacheMap = new HashMap<>(320);
    private static String resultTimeKey = JsonParseUtil.getTimeKey();

    @Override
    @SuppressWarnings("unchecked")
    public void process(String key, Context context, Iterable<Tuple4<String, String, String, String>> input, Collector<Tuple2<String, String>> output) {
        try {
            for (Tuple4<String, String, String, String> tuple : input) {
                String label = tuple.f1;
                //action中某个协议的所有function,如果没有就默认
                String[] metricNames = actionMap.getOrDefault(label, actionMap.get("Default"));
                String dimensions = tuple.f2;
                String message = tuple.f3;
                if (StringUtil.isNotBlank(message)) {
                    Map<String, Object> dimensionsObj = (Map<String, Object>) JsonMapper.fromJsonString(dimensions, Map.class);
                    Map<String, Object> object = (Map<String, Object>) JsonMapper.fromJsonString(message, Map.class);

                    Map<String, Object> cacheMessage = cacheMap.getOrDefault(dimensions, dimensionsObj);
                    for (String name : metricNames) {
                        String[] metrics = metricsMap.get(name);
                        String function = metrics[0];
                        String fieldName = metrics[1];
                        functionSet(function, cacheMessage, name, cacheMessage.get(name), JsonParseUtil.getValue(object, fieldName));

                    }
                    cacheMap.put(dimensions, cacheMessage);
                }
            }

            if (!cacheMap.isEmpty()) {
                Long endTime = context.window().getEnd() / 1000;

                for (String countKey : cacheMap.keySet()) {
                    Map<String, Object> resultMap = cacheMap.get(countKey);
                    JsonParseUtil.setValue(resultMap, resultTimeKey, endTime);
                    output.collect(new Tuple2<>(countKey, JsonMapper.toJsonString(resultMap)));
                }
            }

        } catch (RuntimeException e) {
            logger.error("windows count error,message:" + e);
            e.printStackTrace();
        } finally {
            cacheMap.clear();
        }
    }

    /**
     * 根据schema描述对应字段进行操作的 函数集合
     *
     * @param function       函数名称
     * @param cacheMessage   结果集
     * @param nameValue      当前值
     * @param fieldNameValue 新加值
     */
    private static void functionSet(String function, Map<String, Object> cacheMessage, String resultName, Object nameValue, Object fieldNameValue) {
        switch (function) {
            case "sum":
                cacheMessage.put(resultName, MetricFunctions.longSum(nameValue, fieldNameValue));
                break;
            case "count":
                cacheMessage.put(resultName, MetricFunctions.count(nameValue));
                break;
            case "unique_sip_num":
                //TODO
                break;
            case "unique_cip_num":
                //TODO
                break;
            default:
                break;
        }
    }
}

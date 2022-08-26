package com.zdjizhi.utils.functions.statistics;

import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.general.MetricFunctions;
import com.zdjizhi.utils.general.ParseFunctions;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class FirstCountWindowFunction extends ProcessWindowFunction<Tuple3<String, String, String>, Tuple2<String, String>, String, TimeWindow> {
    private static final Logger logger = LoggerFactory.getLogger(FirstCountWindowFunction.class);

    private HashMap<String, Map<String, Object>> cacheMap = new HashMap<>(16);

    @Override
    @SuppressWarnings("unchecked")
    public void process(String key, Context context, Iterable<Tuple3<String, String, String>> input, Collector<Tuple2<String, String>> output) {
        try {
            HashMap<String, String[]> metricsMap = JsonParseUtil.getMetricFunctionsMap();
            HashMap<String, String[]> actionMap = JsonParseUtil.getActionMap();
            for (Tuple3<String, String, String> tuple : input) {
                String groupKey = tuple.f0;
                String protocol = groupKey.substring(0, groupKey.indexOf("@"));
                String dimensions = tuple.f1;
                String metrics = tuple.f2;
                //action中某个协议的所有action,如果没有就默认
                String[] protocolMetrics = actionMap.getOrDefault(protocol, actionMap.get("Default"));
                if (StringUtil.isNotBlank(metrics)) {
                    Map<String, Object> dimensionsObj = (Map<String, Object>) JsonMapper.fromJsonString(dimensions, Map.class);
                    Map<String, Object> metricsObj = (Map<String, Object>) JsonMapper.fromJsonString(metrics, Map.class);

                    Map<String, Object> cacheMessage = cacheMap.getOrDefault(dimensions, dimensionsObj);
                    for (String protocolMetric : protocolMetrics) {
                        String[] functions = metricsMap.get(protocolMetric);
                        String function = functions[0];
                        String fieldName = functions[1];
                        functionSet(function, cacheMessage, protocolMetric, cacheMessage.get(protocolMetric), JsonParseUtil.getValue(metricsObj, fieldName));

                    }
                    cacheMap.put(dimensions, cacheMessage);
                }
            }

            if (!cacheMap.isEmpty()) {
                for (String dimensions : cacheMap.keySet()) {
                    Map<String, Object> resultMap = cacheMap.get(dimensions);
                    System.out.println("resultMap"+resultMap.toString());
                    output.collect(new Tuple2<>(dimensions, JsonMapper.toJsonString(resultMap)));
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

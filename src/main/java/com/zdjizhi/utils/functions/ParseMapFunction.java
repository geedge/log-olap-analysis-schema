package com.zdjizhi.utils.functions;

import cn.hutool.core.util.RandomUtil;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.general.ParseFunctions;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */
public class ParseMapFunction implements MapFunction<String, Tuple3<String, String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(FirstCountWindowFunction.class);

    @Override
    @SuppressWarnings("unchecked")
    public Tuple3<String, String, String> map(String message) {
        try {
            ArrayList<String[]> jobList = JsonParseUtil.getTransformsList();
            HashMap<String, String> dimensionsMap = JsonParseUtil.getDimensionsMap();
            if (StringUtil.isNotBlank(message)) {
                Map<String, Object> object = (Map<String, Object>) JsonMapper.fromJsonString(message, Map.class);
                Map<String, Object> dimensionsObj = ParseFunctions.transDimensions(dimensionsMap, object);
                if (ParseFunctions.filterLogs(object)) {
                    for (String[] strings : jobList) {
                        //函数名称
                        String function = strings[0];
                        //需要补全的字段的key
                        String resultKeyName = strings[1];
                        //原始日志字段key
                        String logsKeyName = strings[2];
                        //原始日志字段对应的值
                        Object logsKeyValue = JsonParseUtil.getValue(object, strings[2]);
                        //额外的参数的值
                        String parameters = strings[3];

                        switch (function) {
                            case "dismantling":
                                if (StringUtil.isNotBlank(parameters)) {
                                    if (logsKeyValue != null) {
                                        JsonParseUtil.setValue(message, logsKeyName, dismantlingUtils(parameters, logsKeyValue));
                                    }
                                }
                                break;
                            case "combination":
                                if (logsKeyValue != null) {
                                    if (StringUtil.isNotBlank(parameters)) {
                                        combinationUtils(dimensionsObj, object, parameters, resultKeyName, logsKeyName);
                                    }
                                }
                                break;
                            case "flattenSpec":
                                if (logsKeyValue != null) {
                                    if (StringUtil.isNotBlank(parameters)) {
                                        flattenSpec(dimensionsObj, object, parameters, resultKeyName, logsKeyName);
                                    }
                                }
                                break;
                            case "hierarchy":
                                String key = JsonParseUtil.getString(object, logsKeyName) + "@" + ThreadLocalRandom.current().nextInt(StreamAggregateConfig.RANDOM_RANGE_NUM);
                                return new Tuple3<>(key, JsonMapper.toJsonString(dimensionsObj), ParseFunctions.getMetricsLog(object));
                            default:
                                break;
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            logger.error("An error occurred in the original log parsing reorganization,error message is:" + e);
            return new Tuple3<>("", "", "");
        }
        return new Tuple3<>("", "", "");
    }

    /**
     * alignment ID替换操作
     * 根据缓存中的AppId对应信息，获取当前AppId对应的具体名称。
     *
     * @param parameters 参数集
     * @param fieldName  原始日志列名
     */
    private static String dismantlingUtils(String parameters, Object fieldName) {
        String[] alignmentPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        int digits = Integer.parseInt(alignmentPars[0]);
        return fieldName.toString().split(StreamAggregateConfig.PROTOCOL_SPLITTER)[digits];
    }

    /**
     * combination 拼接操作
     * 获取方法函数中 parameters 字段，结构 "parameters": "abc,/" ;abc为要拼接字段 /为拼接的分隔符
     *
     * @param parameters 参数集
     * @param message    原始日志
     * @param fieldName  原始日志列名
     */
    private static void combinationUtils(Map<String, Object> dimensions, Map<String, Object> message, String parameters, String resultKeyName, String fieldName) {
        String[] combinationPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        Object combinationField = JsonParseUtil.getValue(message, combinationPars[0]);
        if (combinationField != null) {
            String separator = combinationPars[1];
            Object fieldNameValue = JsonParseUtil.getValue(message, fieldName);
            if (fieldNameValue != null) {
                String combinationValue = fieldNameValue + separator + combinationField;
                dimensions.put(resultKeyName, combinationValue);
                JsonParseUtil.setValue(message, fieldName, combinationValue);
            } else {
                dimensions.put(resultKeyName, combinationField);
                JsonParseUtil.setValue(message, fieldName, combinationField);
            }
        }
    }

    /**
     * 根据表达式解析json
     *
     * @param message json
     * @param expr    解析表达式
     * @return 解析结果
     */
    private static void flattenSpec(Map<String, Object> dimensions, Map<String, Object> message, String expr, String resultKeyName, String fieldName) {
        try {
            if (StringUtil.isNotBlank(expr)) {
                String operateValue = JsonParseUtil.getString(message, fieldName);
                ArrayList<String> read = JsonPath.parse(operateValue).read(expr);
                if (read.size() >= 1) {
                    String flattenResult = read.get(0);
                    dimensions.put(resultKeyName, flattenResult);
                }
            }
        } catch (ClassCastException | InvalidPathException e) {
            logger.error("设备标签解析异常，[ " + expr + " ]解析表达式错误" + e);
        } catch (RuntimeException e) {
            logger.error("json表达式解析异常，异常信息:" + e);
        }
    }

}

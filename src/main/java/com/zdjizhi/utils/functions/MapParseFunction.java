package com.zdjizhi.utils.functions;

import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.general.ParseFunctions;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */
public class MapParseFunction implements MapFunction<String, Tuple3<String, String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(CountWindowFunction.class);

    private static ArrayList<String[]> jobList = JsonParseUtil.getTransformsList();

    private static HashMap<String, String> dimensionsMap = JsonParseUtil.getDimensionsMap();

    @Override
    @SuppressWarnings("unchecked")
    public Tuple3<String, String, String> map(String message) {
        try {
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
                                if (StringUtil.isNotBlank(parameters)) {
                                    if (logsKeyValue != null) {
                                        combinationUtils(dimensionsObj, object, parameters, resultKeyName, logsKeyName);
                                    }
                                }
                                break;
                            case "hierarchy":
//                                collector.emit(new Values(JsonParseUtil.getValue(object, logsKeyName), JsonMapper.toJsonString(dimensionsObj), JsonMapper.toJsonString(object)));
                                return new Tuple3<>(JsonParseUtil.getString(object, logsKeyName), JsonMapper.toJsonString(dimensionsObj), JsonMapper.toJsonString(object));
                            default:
                                break;
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            logger.error("Map Parse error,message:" + e);
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

}

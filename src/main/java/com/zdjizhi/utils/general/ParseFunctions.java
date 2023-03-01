package com.zdjizhi.utils.general;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.json.JsonParseUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/7/2113:55
 */
public class ParseFunctions {
    private static final Log logger = LogFactory.get();

    /**
     * 构建filters过滤函数，根据Schema指定的函数对日志进行过滤
     *
     * @param object 原始日志
     * @return true or false
     */
    public static boolean filterLogs(Map<String, Object> object) {
        boolean available = false;
        HashMap<String, String> filtersMap = JsonParseUtil.getFiltersMap();
        for (String key : filtersMap.keySet()) {
            switch (key) {
                case "notempty":
                    String value = JsonParseUtil.getString(object, filtersMap.get(key));
                    if (StringUtil.isNotBlank(value)) {
                        available = true;
                    }
                    break;
                default:
            }
        }
        return available;
    }

    /**
     * 解析 dimensions 字段集
     *
     * @param dimensions 维度集
     * @param originalLog    原始日志
     * @return 结果维度集
     */
    public static Map<String, Object> transDimensions(Map<String, String> dimensions, Map<String, Object> originalLog) {
        HashMap<String, Object> dimensionsObj = new HashMap<>(16);

        for (String dimension : dimensions.keySet()) {
            dimensionsObj.put(dimension, JsonParseUtil.getValue(originalLog, dimensions.get(dimension)));
        }

        return dimensionsObj;
    }

    /**
     * 根据原始日志字段，生成schema内指定的metrics指标json。
     *
     * @param originalLog 原始日志json
     * @return 统计metrics json
     */
    public static String getMetricsLog(Map<String, Object> originalLog) {
        Map<String, Object> json = new HashMap<>(16);

        for (String logsKeyName : JsonParseUtil.getMetricsFiledNameList()) {
            json.put(logsKeyName, originalLog.get(logsKeyName));
        }

        return JsonMapper.toJsonString(json);
    }


    /**
     * alignment ID替换操作
     * 根据缓存中的AppId对应信息，获取当前AppId对应的具体名称。
     *
     * @param parameters 参数集
     * @param fieldName  原始日志列名
     */
    public static String dismantlingUtils(String parameters, Object fieldName) {
        String[] alignmentPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        int digits = Integer.parseInt(alignmentPars[0]);
        return fieldName.toString().split(StreamAggregateConfig.PROTOCOL_SPLITTER)[digits];
    }

    /**
     * combination 拼接操作
     * 获取方法函数中 parameters 字段，结构 "parameters": "abc,/" ;abc为要拼接字段 /为拼接的分隔符
     *
     * @param parameters  参数集
     * @param message     原始日志
     * @param logsKeyName 原始日志列名
     */
    public static void combinationUtils(Map<String, Object> dimensions, Map<String, Object> message, String parameters, String resultKeyName, String logsKeyName) {
        String[] combinationPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        String combinationFieldKey = combinationPars[0];
        String separator = combinationPars[1];
        Object combinationFieldValue = JsonParseUtil.getValue(message, combinationFieldKey);
        if (combinationFieldValue != null) {
            Object logsFieldValue = JsonParseUtil.getValue(message, logsKeyName);
            String combinationResult = logsFieldValue + separator + combinationFieldValue;
            JsonParseUtil.setValue(dimensions, resultKeyName, combinationResult);
            JsonParseUtil.setValue(message, logsKeyName, combinationResult);
        }
    }

    /**
     * 根据表达式解析json
     * <p>
     * //* @param message json
     *
     * @param expr 解析表达式
     * @return 解析结果
     */
    public static void flattenSpec(Map<String, Object> dimensions, String expr, String resultKeyName, String logsKeyValue) {
        try {
            if (StringUtil.isNotBlank(expr)) {
                ArrayList<Object> read = JsonPath.parse(logsKeyValue).read(expr);
                if (read.size() >= 1) {
                    String flattenResult = read.get(0).toString();
                    dimensions.put(resultKeyName, flattenResult);
                }
            }
        } catch (RuntimeException e) {
            logger.error("The label resolution exception or [expr] analytic expression error,info:" + e);
        }
    }

}

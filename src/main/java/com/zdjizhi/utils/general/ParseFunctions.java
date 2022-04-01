package com.zdjizhi.utils.general;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.json.JsonParseUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassNameAggregateUtils
 * @Author lixkvip@126.com
 * @Date2020/6/23 14:04
 * @Version V1.0
 **/
public class ParseFunctions {

    /**
     * 解析 dimensions 字段集
     *
     * @param dimensions 维度集
     * @param message    原始日志
     * @return 结果维度集
     */
    public static Map<String, Object> transDimensions(Map<String, String> dimensions, Map<String, Object> message) {
        HashMap<String, Object> dimensionsObj = new HashMap<>(16);

        for (String dimension : dimensions.keySet()) {
            dimensionsObj.put(dimension, JsonParseUtil.getValue(message, dimensions.get(dimension)));
        }

        return dimensionsObj;
    }

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
     * 根据原始日志字段，生成schema内指定的metrics指标json。
     *
     * @param object 原始日志json
     * @return 统计metrics json
     */
    public static String getMetricsLog(Map<String, Object> object) {
        Map<String, Object> json = new HashMap<>(16);

        for (String fileName : JsonParseUtil.getMetricsFiledNameList()) {
            json.put(fileName, object.get(fileName));
        }

        return JsonMapper.toJsonString(json);
    }

}

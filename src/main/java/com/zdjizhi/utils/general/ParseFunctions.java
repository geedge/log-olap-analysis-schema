package com.zdjizhi.utils.general;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.json.JsonParseUtil;

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
     * 获取filters条件map
     */
    private static HashMap<String, String> filtersMap = JsonParseUtil.getFiltersMap();


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

        for (String key : filtersMap.keySet()) {
            switch (key) {
                case "notempty":
                    Object value = JsonParseUtil.getValue(object, filtersMap.get(key));
                    if (value != null && StringUtil.isNotBlank(value.toString())) {
                        available = true;
                    }
                    break;
                default:
            }
        }
        return available;
    }

//    /**
//     * 更新缓存中的对应关系map
//     *
//     * @param hashMap 当前缓存对应关系map
//     */
//    public static void updateAppRelation(HashMap<Integer, String> hashMap) {
//        try {
//            Long begin = System.currentTimeMillis();
//            String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.APP_ID_HTTP);
//            if (StringUtil.isNotBlank(schema)) {
//                String data = JSONObject.parseObject(schema).getString("data");
//                JSONArray objects = JSONArray.parseArray(data);
//                for (Object object : objects) {
//                    JSONArray jsonArray = JSONArray.parseArray(object.toString());
//                    int key = jsonArray.getInteger(0);
//                    String value = jsonArray.getString(1);
//                    if (hashMap.containsKey(key)) {
//                        if (!value.equals(hashMap.get(key))) {
//                            hashMap.put(key, value);
//                        }
//                    } else {
//                        hashMap.put(key, value);
//                    }
//                }
//                logger.warn("更新缓存对应关系用时:" + (begin - System.currentTimeMillis()));
//                logger.warn("更新缓存中的对应的APP关系,拉取接口数据长度:[" + objects.size());
//            }
//        } catch (RuntimeException e) {
//            logger.error("更新缓存APP-ID失败,异常：" + e);
//        }
//    }

}

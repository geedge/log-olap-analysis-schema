package com.zdjizhi.utils.functions.parse;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.general.ParseFunctions;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

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
    private static final Log logger = LogFactory.get();

    @Override
    @SuppressWarnings("unchecked")
    public Tuple3<String, String, String> map(String message) {
        try {
            ArrayList<String[]> jobList = JsonParseUtil.getTransformsList();
            HashMap<String, String> dimensionsMap = JsonParseUtil.getDimensionsMap();
            if (StringUtil.isNotBlank(message)) {
                Map<String, Object> originalLog = (Map<String, Object>) JsonMapper.fromJsonString(message, Map.class);
                Map<String, Object> dimensionsObj = ParseFunctions.transDimensions(dimensionsMap, originalLog);
                if (ParseFunctions.filterLogs(originalLog)) {
                    for (String[] strings : jobList) {
                        //函数名称
                        String function = strings[0];
                        //结果集字段key
                        String resultKeyName = strings[1];
                        //原始日志字段key
                        String logsKeyName = strings[2];
                        //额外的参数的值
                        String parameters = strings[3];

                        //原始日志字段对应的值
                        Object logsValue = JsonParseUtil.getValue(originalLog, logsKeyName);

                        switch (function) {
                            case "combination":
                                if (logsValue != null) {
                                    if (StringUtil.isNotBlank(parameters)) {
                                        ParseFunctions.combinationUtils(dimensionsObj, originalLog, parameters, resultKeyName, logsKeyName);
                                    }
                                }
                                break;
                            case "flattenSpec":
                                if (logsValue != null) {
                                    if (StringUtil.isNotBlank(parameters)) {
                                        ParseFunctions.flattenSpec(dimensionsObj, parameters, resultKeyName, logsValue.toString());
                                    }
                                }
                                break;
                            case "hierarchy":
                                String key = JsonParseUtil.getString(dimensionsObj, resultKeyName) + "@" + ThreadLocalRandom.current().nextInt(StreamAggregateConfig.RANDOM_RANGE_NUM);
                                return new Tuple3<>(key, JsonMapper.toJsonString(dimensionsObj), ParseFunctions.getMetricsLog(originalLog));
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

}

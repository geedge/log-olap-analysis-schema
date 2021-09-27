package com.zdjizhi.utils.functions;

import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.util.Map;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/7/2117:32
 */
public class MyTimeAssigner implements SerializableTimestampAssigner<String> {
    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        Map<String, Object> object = (Map<String, Object>) JsonMapper.fromJsonString(element, Map.class);

        return JsonParseUtil.getLong(object,"common_end_time");
    }
}

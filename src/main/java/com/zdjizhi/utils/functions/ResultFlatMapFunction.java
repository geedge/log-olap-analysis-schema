package com.zdjizhi.utils.functions;

import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/7/2114:52
 */
public class ResultFlatMapFunction implements FlatMapFunction<String, String> {
    private static String[] jobList = JsonParseUtil.getHierarchy();

    @Override
    @SuppressWarnings("unchecked")
    public void flatMap(String value, Collector out) throws Exception {
        StringBuffer stringBuffer = new StringBuffer();
        String name = jobList[0];
        Map<String, Object> jsonObject = (Map<String, Object>) JsonMapper.fromJsonString(value, Map.class);
        String protocol = JsonParseUtil.getString(jsonObject, name);
        if (StringUtil.isNotBlank(protocol)) {
            String[] protocolIds = protocol.split(StreamAggregateConfig.PROTOCOL_SPLITTER);
            for (String proto : protocolIds) {
                if (StringUtil.isBlank(stringBuffer.toString())) {
                    stringBuffer.append(proto);
                    jsonObject.put(name, stringBuffer.toString());
                    out.collect(JsonMapper.toJsonString(jsonObject));
                } else {
                    stringBuffer.append(jobList[1]).append(proto);
                    jsonObject.put(name, stringBuffer.toString());
                    out.collect(JsonMapper.toJsonString(jsonObject));
                }
            }
//            out.collect(value);
        }
    }
}

package com.zdjizhi.utils.functions;

import cn.hutool.core.util.RandomUtil;
import com.zdjizhi.common.StreamAggregateConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/7/2112:13
 */
public class FirstKeyByFunction implements KeySelector<Tuple4<String, String, String, String>, String> {

    @Override
    public String getKey(Tuple4<String, String, String, String> value) throws Exception {
//        //以map拼接的key分组
        return value.f0;
    }
}

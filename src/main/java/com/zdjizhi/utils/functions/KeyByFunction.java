package com.zdjizhi.utils.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/7/2112:13
 */
public class KeyByFunction implements KeySelector<Tuple3<String, String, String>, String> {

    @Override
    public String getKey(Tuple3<String, String, String> value) throws Exception {
        //以map拼接的key分组
        return value.f1;
    }
}

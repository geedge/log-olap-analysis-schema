package com.zdjizhi.utils.general;


import com.zdjizhi.utils.json.JsonTypeUtils;

/**
 * @author qidaijie
 * @Package com.zdjizhi.storm.utils.general
 * @Description:
 * @date 2021/7/2015:31
 */
public class MetricFunctions {
    /**
     * Long类型的数据求和
     *
     * @param value1 第一个值
     * @param value2 第二个值
     * @return value1 + value2
     */
    public static Long longSum(Object value1, Object value2) {
        Long res1 = JsonTypeUtils.checkLongValue(value1);
        Long res2 = JsonTypeUtils.checkLongValue(value2);

        return res1 + res2;
    }

    /**
     * 计算Count
     *
     * @param count 当前count值
     * @return count+1
     */
    public static Long count(Object count) {

        return JsonTypeUtils.checkLongValue(count) + 1L;
    }
}

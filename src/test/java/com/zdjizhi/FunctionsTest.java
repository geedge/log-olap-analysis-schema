package com.zdjizhi;

import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.json.JsonParseUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author qidaijie
 * @Package com.zdjizhi
 * @Description:
 * @date 2021/9/1714:22
 */
public class FunctionsTest {
    private static HashMap<String, String[]> metricsMap = JsonParseUtil.getMetricsMap();

    @Test
    public void actionTest() {
       HashMap<String, String[]> actionMap = JsonParseUtil.getActionMap();
        String[] metricNames = actionMap.getOrDefault("", actionMap.get("Default"));
        System.out.println(actionMap.toString());
        System.out.println(Arrays.toString(metricNames));



    }



}

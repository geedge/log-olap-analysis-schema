package com.zdjizhi.topology;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.functions.*;
import com.zdjizhi.utils.kafka.Consumer;
import com.zdjizhi.utils.kafka.Producer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/**
 * @author qidaijie
 * @Package com.zdjizhi.topology
 * @Description:
 * @date 2021/5/2016:42
 */
public class StreamAggregateTopology {
    private static final Log logger = LogFactory.get();

    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//            environment.enableCheckpointing(5000);

            DataStream<String> streamSource = environment.addSource(Consumer.getKafkaConsumer())
                    .setParallelism(StreamAggregateConfig.CONSUMER_PARALLELISM);

            SingleOutputStreamOperator<Tuple3<String, String, String>> parseDataMap = streamSource.map(new MapParseFunction()).name("ParseDataMap")
                    .setParallelism(StreamAggregateConfig.PARSE_PARALLELISM);

            WindowedStream<Tuple3<String, String, String>, String, TimeWindow> window = parseDataMap.keyBy(new KeyByFunction())
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(StreamAggregateConfig.COUNT_WINDOW_TIME)));

            SingleOutputStreamOperator<String> metricCountWindow = window.process(new CountWindowFunction()).name("MetricCountWindow")
                    .setParallelism(StreamAggregateConfig.PARSE_PARALLELISM);

            metricCountWindow.flatMap(new ResultFlatMapFunction()).name("ResultFlatMap").setParallelism(StreamAggregateConfig.PARSE_PARALLELISM)
                    .addSink(Producer.getKafkaProducer()).name("LogSinkKafka").setParallelism(StreamAggregateConfig.PARSE_PARALLELISM);

            environment.execute(args[0]);
        } catch (Exception e) {
            logger.error("This Flink task start ERROR! Exception information is :" + e);
        }

    }

}

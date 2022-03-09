package com.zdjizhi.topology;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.StreamAggregateConfig;
import com.zdjizhi.utils.functions.*;
import com.zdjizhi.utils.kafka.KafkaConsumer;
import com.zdjizhi.utils.kafka.KafkaProducer;
import org.apache.flink.api.java.tuple.Tuple2;
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

            //两个输出之间的最大时间 (单位milliseconds)
            environment.setBufferTimeout(StreamAggregateConfig.BUFFER_TIMEOUT);

            DataStream<String> streamSource = environment.addSource(KafkaConsumer.getKafkaConsumer())
                    .setParallelism(StreamAggregateConfig.SOURCE_PARALLELISM);

            SingleOutputStreamOperator<Tuple3<String, String, String>> parseDataMap = streamSource.map(new ParseMapFunction())
                    .name("ParseDataMap")
                    .setParallelism(StreamAggregateConfig.PARSE_PARALLELISM);

            WindowedStream<Tuple3<String, String, String>, String, TimeWindow> firstWindow = parseDataMap.keyBy(new FirstKeyByFunction())
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(StreamAggregateConfig.FIRST_COUNT_WINDOW_TIME)));

            SingleOutputStreamOperator<Tuple2<String, String>> metricCountWindow = firstWindow.process(new FirstCountWindowFunction())
                    .name("FirstCountWindow")
                    .setParallelism(StreamAggregateConfig.FIRST_WINDOW_PARALLELISM);

            WindowedStream<Tuple2<String, String>, String, TimeWindow> secondWindow = metricCountWindow.keyBy(new SecondKeyByFunction())
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(StreamAggregateConfig.SECOND_COUNT_WINDOW_TIME)));

            SingleOutputStreamOperator<String> secondCountWindow = secondWindow.process(new SecondCountWindowFunction())
                    .name("SecondCountWindow").setParallelism(StreamAggregateConfig.SECOND_WINDOW_PARALLELISM);

            secondCountWindow.flatMap(new ResultFlatMapFunction()).name("ResultFlatMap").setParallelism(StreamAggregateConfig.SINK_PARALLELISM)
                    .addSink(KafkaProducer.getKafkaProducer()).name("LogSinkKafka").setParallelism(StreamAggregateConfig.SINK_PARALLELISM);

            environment.execute(args[0]);
        } catch (Exception e) {
            logger.error("This Flink task start ERROR! Exception information is :" + e);
        }

    }

}

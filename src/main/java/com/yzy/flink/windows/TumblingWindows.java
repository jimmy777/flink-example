package com.yzy.flink.windows;

import com.sun.java.swing.plaf.windows.resources.windows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description Tumbling Windows 滚动窗口的例子。
 * <p>
 * 注意：
 * 1. 固定时间大小的窗口。
 * 2.
 *
 */
public class TumblingWindows {

    public static void main(String[] args) throws Exception {

        //--- env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 指定使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //--- source
        KeyedStream<String, String> keyedStream = env.addSource(new SourceFunction<String>() {

            private boolean isCancel = true;

            /**
             * 字符串的 source 源模拟产生数据，记录格式如下：
             * 1607654263504	CH	1
             */
            @Override
            public void run(SourceContext<String> ctx) throws Exception {

                int num = 1;

                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    String msg = currentTimeMillis + "\tCH\t" + num;
                    // System.out.println("<source> " + msg);
                    num++;
                    ctx.collect(msg);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            /**
             * 基于 EventTime 时间升序来插入 watermark 时间戳标记的。
             *
             * @param element 即一条记录，可以提取 EventTime，并且来自动计算出 watermark 时间戳。
             * @return watermark 时间戳 = element 的 Timestamp （即 EventTime） - 1
             */
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] split = element.split("\t");
                return Long.parseLong(split[0]);
            }
        }).keyBy(new KeySelector<String, String>() {
            /**
             * 进行 KeyBy 操作，按 key 进行分区处理。
             *
             * @param value 表示一条记录，可以提取 key 字段。例子中即 CH 字段。
             * @return 返回 key 字段。
             */
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split("\t");
                return split[1];
            }
        });


//      Tumbling Windows 两种事件操作：
//      1. TumblingEventTimeWindows 使用 EventTime 时间计算 Watermark 时间来触发 windows 窗口计算。
//      2. TumblingProcessingTimeWindows 会让 WaterMark 不好使了，而是使用系统时间来触发 windows 窗口计算。

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {


                    /**
                     * @param key        是 keyby() 函数分区的的 key。
                     * @param context    是 subtask 环境。
                     * @param elements   是所有窗口的记录。
                     * @param out        发生到下游 operator。
                     */
                    @Override
                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        System.out.println(
                                "--> (process) subtask: " + getRuntimeContext().getIndexOfThisSubtask() +
                                        "| start: " + context.window().getStart() +
                                        "| end: " + context.window().getEnd() +
                                        "| watermark: " + context.currentWatermark() +
                                        "| currentTime: " + System.currentTimeMillis()
                        );

                        Iterator<String> iterator = elements.iterator();
                        int sum = 0;
                        for (;iterator.hasNext();) {
                            String next = iterator.next();
                            System.out.println("--> " + next);
                            String[] split = next.split("\t");
                            sum += Integer.parseInt(split[2]);
                        }
                        out.collect("----> sum: " + sum);
                    }
                }).print();


        //--- 注意: sink 并不是 action，Flink 没有 sink 也能提交任务去跑。只要有下面的语句 env.execute() 就可以。

        //--- submit
        env.execute("my job.");
    }
}

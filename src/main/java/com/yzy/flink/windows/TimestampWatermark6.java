package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;
import java.util.Random;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description 基于 EventTime 时间，数据延迟的场景下的例子。
 * <p>
 * 注意：
 * 1. watermark 时间是最后一条记录的 EventTime-1，即 end 时间-1。
 * 2. 大于 watermark 时间的记录将无法进入窗口进行计算。但可以进入下一个窗口参与计算。
 * 3. 记录要有进入 window 窗口的时间才行。
 *
 * 扩展一：可以设置一个延迟时间（s）的函数 allowedLateness() 来确保迟到一点点的数据可以进入窗口参与计算。
 * 扩展二：使用 sideOutputLateData() 分流器，可以单独来单独处理延迟的数据。OutputTag 为延迟记录的标记，以便后面的分流器进行处理。
 *
 */
public class TimestampWatermark6 {

    /** 给记延迟的记录的加上一个标签 */
    private static final OutputTag<String> LATE_TAG = new OutputTag<>("late", BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {

        //--- env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 指定使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 并发度为1，便于观察结果。
        env.setParallelism(1);


        //--- source
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = env.addSource(new SourceFunction<String>() {

            private boolean isCancel = true;


            /**
             * 字符串的 source 源模拟产生数据，记录格式如下：
             * 1607654263504	CH	1
             *
             */
            @Override
            public void run(SourceContext<String> ctx) throws Exception {

                int num = 1;

                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    if (num % 2 == 0) {
                        currentTimeMillis -= 4000;
                    }
                    String msg = currentTimeMillis + "\tCH\t" + num;
                    System.out.println("<source> " + msg);
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
        });


        SingleOutputStreamOperator<String> process = stringSingleOutputStreamOperator.keyBy(new KeySelector<String, String>() {
            /**
             * 进行 KeyBy 操作，按 key 进行分区处理。
             *
             * @param value 表示一条记录，可以提取 key 字段。例子中即 CH 字段。
             * @return 返回 key 字段。
             *
             */
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split("\t");
                return split[1];
            }
        }).timeWindow(Time.seconds(2)) // 时间窗口函数，窗口期为2秒钟一个窗口。
                .allowedLateness(Time.seconds(2))  // 扩展一：允许延时的时间设置（s）确保延迟一点点的记录进入窗口参与计算。
                .sideOutputLateData(LATE_TAG) // 扩展二：定义一个分流器处理延迟的记录。
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    /**
                     * 窗口处理函数
                     *
                     * @param key 获取 key
                     * @param context 可以获取 subtask 的信息
                     * @param elements 进入窗口的记录集，可以迭代处理每个记录
                     * @param out 可以继续向下游发射数据
                     */
                    @Override
                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        // 打印一下当前窗口里面的一些信息。
                        System.out.println(
                                "--> (process) subtask: " + getRuntimeContext().getIndexOfThisSubtask() +
                                        "| start: " + context.window().getStart() +
                                        "| end: " + context.window().getEnd() +
                                        "| watermark: " + context.currentWatermark() +
                                        "| currentTime: " + System.currentTimeMillis()
                        );

                        // 遍历一下进入窗口里面的记录都有哪些。
                        Iterator<String> iterator = elements.iterator();
                        for (; iterator.hasNext(); ) {
                            String next = iterator.next();
                            System.out.println("----> [windows] " + next);
                            // 将正确的记录
                            out.collect("----> $$$ on time record: " + next);
                        }

                        System.out.println("\n\n");
                    }
                });

        // 打印正常进入窗口的数据。
        process.print();

        // 得到分流器出来的数据集，利用map进行打印显示。
        DataStream<String> lateDataStream = process.getSideOutput(LATE_TAG);
        lateDataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "---->>>> late record: " + value;
            }
        }).print();


        //--- 注意: sink 并不是 action，Flink 没有 sink 也能提交任务去跑。只要有下面的语句 env.execute() 就可以。

        //--- submit
        env.execute("my job.");
    }
}

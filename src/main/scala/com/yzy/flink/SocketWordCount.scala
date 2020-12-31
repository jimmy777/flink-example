package com.yzy.flink



import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


/**
 *
 * @author Y.Z.Y
 * @date 2020/11/19 10:31
 * @description None.
 */
object SocketWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    val text: DataStream[String] = env.socketTextStream("192.168.209.101", 6666)

    // 序列化转换
    import org.apache.flink.api.scala._

    // 实现（一）：函数
    //val wordCount: DataStream[(String, Int)] = text.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)

    // 实现（二）：用匿名类
    val wordCount: DataStream[(String, Int)] = text.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        val strings: Array[String] = value.split(" ")
        for(s <- strings) {
          out.collect(s)
        }
      }
    }).map((_, 1)).keyBy(_._1).sum(1)

    wordCount.print()

    env.execute("socket word count")

  }
}

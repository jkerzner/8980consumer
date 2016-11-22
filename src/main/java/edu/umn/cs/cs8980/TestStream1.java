package edu.umn.cs.cs8980;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class TestStream1 {

	public static void main(String[] args) throws Exception {

                Properties prop = new Properties();
                InputStream propStream = new FileInputStream("src/main/resources/config.properties");
                prop.load(propStream);

                // Setup the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                
                // Setup the time characteristic - either processing time or event time.
                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                
                // get events as they arrive
                DataStreamSource<String> sourceStream = env
                    .socketTextStream(prop.getProperty("generatorHost"), Integer.parseInt(prop.getProperty("generatorPort")));
                
                // the transformed stream contains watermarks
                DataStream<Tuple2<Long, Integer>> transformedStream = sourceStream
                        .map(new TimeGetter())
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple2<Long, Integer> t) {
                                System.err.println("Stamping " + t.f1 + " at " + t.f0);
                                return t.f0;
                            }
                        });

                // turn the raw input stream into a windowed stream
                transformedStream
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .allowedLateness(Time.seconds(2))
                        .sum(1)
                        .printToErr();
                
		// execute program
		env.execute("Streaming with timestamps");
	}
        
    private static class TimeGetter implements MapFunction<String, Tuple2<Long, Integer>> {
        
        @Override
        public Tuple2<Long, Integer> map(String value) throws Exception {
            Tuple2<Long, Integer> rv =  new Tuple2<>();
            System.err.println(value);
            
            Pattern p = Pattern.compile("^(\\d+)\\W+(\\d+)$");
            Matcher m = p.matcher(value);
            m.find();
            
            rv.f0 = Long.parseLong(m.group(1));
            rv.f1 = Integer.parseInt(m.group(2));
            
            return rv;
        }
    }
    
    private class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple2<Long, Integer>>, Serializable {
        private static final long serialVersionUID = 17274953L;

        private long currentWatermarkTimestamp = 0L;
        
        @Override
        public Watermark getCurrentWatermark() {
            if (this.currentWatermarkTimestamp <= 0) {
                return new Watermark(Long.MIN_VALUE);
            }
            return new Watermark(this.currentWatermarkTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, Integer> t, long l) {
            this.currentWatermarkTimestamp = t.f0;
            return this.currentWatermarkTimestamp;
        }
    }
}

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<WikipediaAnalysis.WikipediaEditEvent> {

    private final long maxOutOfOrderness = 3500;

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(WikipediaAnalysis.WikipediaEditEvent element, long previousElementTimestamp) {
        long timestamp = element.getCreateTime();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

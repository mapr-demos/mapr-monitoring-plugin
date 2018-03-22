package com.mapr.monitoring.web;

import com.mapr.monitoring.reader.TopicReader;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final TopicReader reader;

    @RequestMapping("/read")
    public String readTopic(@RequestParam("stream") String metricStream, @RequestParam("host") String host,
            @RequestParam("topic") String topic) {
        reader.readTopic(metricStream, host, topic);
        return "OK";
    }
}

package com.mapr.monitoring.web;

import com.mapr.monitoring.reader.TopicReader;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final TopicReader reader;

    @RequestMapping("/read")
    public String readTopic() {
        reader.readTopic();
        return "OK";
    }
}

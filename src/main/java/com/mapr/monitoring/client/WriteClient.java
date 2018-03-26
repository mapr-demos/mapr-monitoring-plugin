package com.mapr.monitoring.client;

import feign.Headers;
import feign.RequestLine;

public interface WriteClient {
    @RequestLine("POST /write")
    @Headers("Content-Type: application/octet-stream")
    String writeMetric(String body);
}

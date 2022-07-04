package com.lne.elastic.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.http.HttpHeaders;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Configuration
public class RestClientConfig extends AbstractElasticsearchConfiguration {
    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        HttpHeaders titleHeaders = new HttpHeaders();
        titleHeaders.add("some-header","hello word");
        final ClientConfiguration config = ClientConfiguration.builder()
                .connectedTo("192.168.2.131:9200")
                .withConnectTimeout(Duration.ofSeconds(5))
                .withSocketTimeout(Duration.ofSeconds(3))
                .withDefaultHeaders(titleHeaders)
                .withHeaders(() ->{
                    HttpHeaders httpHeaders = new HttpHeaders();
                    httpHeaders.add("currentTime", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    return httpHeaders;
                })
                .withClientConfigurer(RestClients.RestClientConfigurationCallback.from(httpAsyncClientBuilder ->
                {return httpAsyncClientBuilder;}))  // 设置非反应式客户端功能
                .build();
        return RestClients.create(config).rest();
    }
}

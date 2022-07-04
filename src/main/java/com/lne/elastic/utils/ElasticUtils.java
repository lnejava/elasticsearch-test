package com.lne.elastic.utils;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class ElasticUtils {

    @Autowired
    private RestHighLevelClient client;

    /**
     * elastic批量操作数据
     * @param indexName：索引
     * @param type：操作类型（index，delete，create，。。。）
     * @param list：数据集合，Map<string,Object>,string为添加的document的id
     */
    public void bulkData(String indexName,String type,List<Map<String,Object>> list){
        if(null == list || list.size()<=0){
           log.warn("参数list为null");
           return;
        }
        if(indexName.isBlank() || type.isBlank()){
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        for(Map<String,Object> map:list){
           Set<String> keys =  map.keySet();
            String id = null;
            for (String key : keys){
               id = "id".equals(key)?key: String.valueOf(UUIDs.randomBase64UUIDSecureString());
           }
                IndexRequest indexRequest = new IndexRequest();
                indexRequest.id(id);
                indexRequest.source(map, XContentType.JSON);
                indexRequest.opType(type);
                bulkRequest.add(indexRequest);
        }
        bulkRequest.timeout("2m");
        bulkRequest.setRefreshPolicy("wait_for");

        //发送请求：同步请求
        BulkResponse bulk = null;
        try {
            log.info("开始发送批量添加数据请求");
            bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);

            // 处理响应
            if(bulk != null){
                for(BulkItemResponse itemResponse:bulk){
                    DocWriteResponse response = itemResponse.getResponse();
                    if(itemResponse.getOpType() == DocWriteRequest.OpType.INDEX ||
                            itemResponse.getOpType() == DocWriteRequest.OpType.CREATE){
                        IndexResponse indexResponse = (IndexResponse) response;
                        log.info("新增成功，{}"+indexResponse.toString());
                    }else if(itemResponse.getOpType() == DocWriteRequest.OpType.UPDATE){
                        UpdateResponse updateResponse = (UpdateResponse) response;
                        log.info("修改成功，{}"+updateResponse.toString());
                    }else if(itemResponse.getOpType() == DocWriteRequest.OpType.DELETE){
                        DeleteResponse deleteResponse = (DeleteResponse) response;
                        log.info("删除成功，{}"+deleteResponse.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

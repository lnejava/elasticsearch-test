package com.lne.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lne.elastic.beans.Product;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.*;

@SpringBootTest
@Slf4j
class ElasticsearchTestApplicationTests {

	@Autowired
	private RestHighLevelClient highLevelClient;
	@Autowired
	private ObjectMapper objectMapper;

	/**
	 * 创建索引
	 */
	@Test
	public void testCreateIndex(){
		CreateIndexRequest index1 = new CreateIndexRequest("index1");
		CreateIndexResponse createIndexResponse = null;
		try {
			log.info("=======开始请求创建index===============");
			createIndexResponse = highLevelClient.indices().create(index1, RequestOptions.DEFAULT);
			log.info("==========index创建成功=================",createIndexResponse.isAcknowledged());
		} catch (IOException e) {
			log.warn("index创建失败",e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * 删除索引
	 */
	@Test
	public void testDeleteIndex(){
		DeleteIndexRequest index1 = new DeleteIndexRequest("index1");
		try {
			log.info("开始删除索引");
			AcknowledgedResponse delete = highLevelClient.indices().delete(index1, RequestOptions.DEFAULT);
			log.info("删除索引成功");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 添加文档
	 */
	@Test
	public void testAddDocument() throws IOException {
		Product product = new Product(5, "X11", 2050.0, "比荣耀X10贵");
		//构建IndexRequest对象
		IndexRequest indexRequest = new IndexRequest("index1");
		indexRequest.id(product.getProductId()+"");
		indexRequest.source(objectMapper.writeValueAsString(product), XContentType.JSON);
		log.info("开始添加document");
		IndexResponse index = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
		log.info(index.toString());
	}

	/**
	 * 检索数据
	 */
	@Test
	public void testSearch() throws IOException {
		int pageSize = 10;
		int pageNum = 1;
		// 用于封装检索条件
		SearchRequest searchRequest = new SearchRequest();

		// 用于构建查询条件
		SearchSourceBuilder builder = new SearchSourceBuilder();
//		builder.query(QueryBuilders.matchAllQuery());  // 查询所有
//		builder.query(QueryBuilders.matchQuery("productName","哇哈哈"));	// 根据关键字查询
		builder.query(QueryBuilders.multiMatchQuery("荣耀","productName","productDescribe")); // 多个字段条件查询(相当于or)

		// 分页查询
		builder.from((pageNum-1)*pageSize);
		builder.size(pageSize);

		// 高亮显示
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		HighlightBuilder.Field field = new HighlightBuilder.Field("productName");
		highlightBuilder.field(field);
		highlightBuilder.preTags("<label style='color:red'>");
		highlightBuilder.postTags("</label>");
		builder.highlighter(highlightBuilder);

		searchRequest.source(builder);
		SearchResponse search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

		// 查看查询命中数据
		SearchHits hits = search.getHits();
//		hits.forEach(hit ->System.out.println(hit));

		// 封装查询数据
		List<Product> products = new ArrayList<>();
		Iterator<SearchHit> iterator = hits.iterator();
		while (iterator.hasNext()){
			SearchHit searchHit = iterator.next();
			String sourceAsString = searchHit.getSourceAsString();
			Product product = objectMapper.readValue(sourceAsString, Product.class);
			// 处理高亮字段
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			HighlightField productName = highlightFields.get("productName");
			if(productName != null){
				String highlight = Arrays.toString(productName.fragments());
				product.setProductName(highlight);
			}
			products.add(product);
		}
		System.out.println(products);
	}
}

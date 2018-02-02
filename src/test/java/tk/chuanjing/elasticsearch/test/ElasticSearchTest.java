package tk.chuanjing.elasticsearch.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import tk.chuanjing.elasticsearch.domain.Article;

/**
 * ElasticSearch 测试程序
 * 
 * @author ChuanJing
 * @date 2018年1月29日 上午1:27:36
 * @version 1.0
 */
public class ElasticSearchTest {
	
	/**
	 * 创建连接搜索服务器对象
	 * @return
	 * @throws UnknownHostException
	 */
	private Client buildClient() throws UnknownHostException {
		// 创建连接搜索服务器对象
		Client client = TransportClient.builder()
									   .build()
									   .addTransportAddress( new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300) );
		return client;
	}
	
	/**
	 * 打印查询结果
	 * @param searchResponse
	 */
	private void printSearchResponse(SearchResponse searchResponse) {
		SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
		System.out.println("查询结果有：" + hits.getTotalHits() + "条");
		
		Iterator<SearchHit> iterator = hits.iterator();
		while (iterator.hasNext()) {
			SearchHit searchHit = iterator.next(); // 每个查询对象
			System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
			System.out.println("title:" + searchHit.getSource().get("title"));
		}
	}

	/**
	 * 直接在ElasticSearch中建立文档，自动创建索引
	 */
	@Test
	public void demo1() throws IOException {
		Client client = buildClient();
		/*
		 * 描述json 数据
		 * {id:xxx, title:xxx, content:xxx}
		 */
		XContentBuilder builder = XContentFactory.jsonBuilder()
												 .startObject()
												 .field("id", 1)
												 .field("title", "ElasticSearch是什么")
												 .field("content", "ElasticSearch是一个基于Lucene的搜索服务器，它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
												 .endObject();
		
		// 建立文档对象
		client.prepareIndex("blog1", "article", "1").setSource(builder).get();
		
		// 关闭连接
		client.close();
	}
	
	/**
	 * 搜索在elasticSearch中创建的文档对象
	 * @throws IOException
	 */
	@Test
	public void demo2() throws IOException {
		Client client = buildClient();
		
		/*
		 * 搜索数据
		 * QueryBuilders中常用查询方法：
		 * 		boolQuery() 布尔查询，可以用来组合多个查询条件
		 * 		fuzzyQuery() 相似度查询
		 * 		matchAllQuery() 查询所有数据
		 * 		regexpQuery() 正则表达式查询
		 * 		termQuery() 词条查询
		 * 		wildcardQuery() 模糊查询
		 * 
		 * get() === execute().actionGet()
		 */
		SearchResponse searchResponse = client.prepareSearch("blog1")
											  .setTypes("article")
											  .setQuery(QueryBuilders.matchAllQuery())
											  .get();
		
		printSearchResponse(searchResponse);
		
		// 关闭连接
		client.close();
	}

	/**
	 * 各种查询使用
	 * @throws IOException
	 */
	@Test
	public void demo3() throws IOException {
		Client client = buildClient();
		/*
		SearchResponse searchResponse = client.prepareSearch("blog1")
											  .setTypes("article")
											  .setQuery(QueryBuilders.queryStringQuery("全面"))
											  .get();
		
		SearchResponse searchResponse = client.prepareSearch("blog1")
											  .setTypes("article")
											  .setQuery(QueryBuilders.wildcardQuery("content", "*全文*"))
											  .get();
		*/
		
		SearchResponse searchResponse = client.prepareSearch("blog1")
											  .setTypes("article")
											  .setQuery(QueryBuilders.termQuery("content", "搜索"))
											  .get();
		
		printSearchResponse(searchResponse);
		
		// 关闭连接
		client.close();
	}
	
	/**
	 * 索引操作：创建、删除
	 * @throws IOException
	 */
	@Test
	public void demo4() throws IOException {
		Client client = buildClient();

		// 创建索引
		client.admin().indices().prepareCreate("blog2").get();

		// 删除索引
		// client.admin().indices().prepareDelete("blog2").get();

		// 关闭连接
		client.close();
	}
	
	/**
	 * 映射操作：添加映射
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void demo5() throws IOException, InterruptedException, ExecutionException {
		Client client = buildClient();

		// 添加映射
		XContentBuilder builder = XContentFactory.jsonBuilder()
				.startObject()
					.startObject("article")
						.startObject("properties")
							.startObject("id")
								.field("type", "integer")
								.field("store", "yes")
							.endObject()
							.startObject("title")
								.field("type", "string")
								.field("store", "yes")
								.field("analyzer", "ik")
							.endObject()
							.startObject("content")
								.field("type", "string")
								.field("store", "yes")
								.field("analyzer", "ik")
							.endObject()
						.endObject()
					.endObject()
				.endObject();

		PutMappingRequest mapping = Requests.putMappingRequest("blog2")
											.type("article")
											.source(builder);
		
		client.admin().indices().putMapping(mapping).get();

		// 关闭连接
		client.close();
	}
	
	/**
	 * 文档相关操作
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void demo6() throws IOException, InterruptedException, ExecutionException {
		Client client = buildClient();
		
		// 描述json 数据
		/*
		 * {id:xxx, title:xxx, content:xxx}
		 */
		Article article = new Article();
		article.setId(2);
		article.setTitle("搜索工作其实很快乐");
		article.setContent("我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

		ObjectMapper objectMapper = new ObjectMapper();

		// 建立文档
		client.prepareIndex("blog2", "article", article.getId().toString())
			  .setSource(objectMapper.writeValueAsString(article))
			  .get();

		// 修改文档
//		client.prepareUpdate("blog2", "article", article.getId().toString())
//			  .setDoc(objectMapper.writeValueAsString(article))
//			  .get();

		// 修改文档
//		client.update(new UpdateRequest("blog2", "article", article.getId().toString()).doc(objectMapper.writeValueAsString(article)))
//			  .get();

		// 删除文档
//		client.prepareDelete("blog2", "article", article.getId().toString())
//		 	  .get();

		// 删除文档
//		client.delete(new DeleteRequest("blog2", "article", article.getId().toString()))
//			  .get();

		// 关闭连接
		client.close();
	}
	
	/**
	 * 批量创建100条记录，用于演示分页查询
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void demo7() throws IOException, InterruptedException, ExecutionException {
		Client client = buildClient();

		ObjectMapper objectMapper = new ObjectMapper();

		for (int i = 1; i <= 100; i++) {
			// 描述json 数据
			Article article = new Article();
			article.setId(i);
			article.setTitle(i + "搜索工作其实很快乐");
			article.setContent(i + "我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

			// 建立文档
			client.prepareIndex("blog2", "article", article.getId().toString())
				  .setSource(objectMapper.writeValueAsString(article))
				  .get();
		}
		
		// 关闭连接
		client.close();
	}
	
	/**
	 * 分页搜索
	 * @throws IOException
	 */
	@Test
	public void demo8() throws IOException {
		Client client = buildClient();
		
		// 搜索数据
		// get() === execute().actionGet()
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog2")
														  .setTypes("article")
														  .setQuery(QueryBuilders.matchAllQuery());

		// 查询第2页数据，每页20条
		searchRequestBuilder.setFrom(20).setSize(20);

		SearchResponse searchResponse = searchRequestBuilder.get();
		printSearchResponse(searchResponse);

		// 关闭连接
		client.close();
	}
	
	/**
	 * 文档查询结果高亮显示
	 * @throws IOException
	 */
	@Test
	public void demo9() throws IOException {
		Client client = buildClient();

		ObjectMapper objectMapper = new ObjectMapper();

		// 搜索数据
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog2")
														  .setTypes("article")
														  .setQuery(QueryBuilders.termQuery("title", "搜索"));

		// 高亮定义
		searchRequestBuilder.addHighlightedField("title"); 		// 对title字段进行高亮
		searchRequestBuilder.setHighlighterPreTags("<em>"); 	// 前置元素
		searchRequestBuilder.setHighlighterPostTags("</em>");	// 后置元素

		// 去搜索
		SearchResponse searchResponse = searchRequestBuilder.get();

		// 处理搜索结果
		SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
		System.out.println("查询结果有：" + hits.getTotalHits() + "条");
		Iterator<SearchHit> iterator = hits.iterator();
		while (iterator.hasNext()) {
			SearchHit searchHit = iterator.next(); // 每个查询对象

			// 将高亮处理后内容，替换原有内容 （原有内容，可能会出现显示不全 ）
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			HighlightField titleField = highlightFields.get("title");

			// 获取到原有内容中 每个高亮显示 集中位置 fragment 就是高亮片段
			Text[] fragments = titleField.fragments();
			String title = "";
			for (Text text : fragments) {
				title += text;
			}
			
			// 将查询结果转换为对象
			Article article = objectMapper.readValue(searchHit.getSourceAsString(), Article.class);

			// 用高亮后内容，替换原有内容
			article.setTitle(title);

			System.out.println(article);
		}

		// 关闭连接
		client.close();
	}
}

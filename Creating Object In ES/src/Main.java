import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Main {

    public static void main(String[] args) {

//ИНДЕКСИРОВАНИЕ ДАННЫХ С ПОМОЩЬЮ ELASTICSEARCH
        try {
//ПОДКЛЮЧЕНИЕ ES
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9300, "http")));
//ЗАПРОС: ЗДОРОВЬЕ КЛАСТЕРА
            ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
//ОТВЕТ: ЗДОРОВЬЕ КЛАСТЕРА
            ClusterHealthResponse health = client.cluster().health(clusterHealthRequest, RequestOptions.DEFAULT);
            System.out.println(health.toString());
//СОЗДАНИЕ ИНДЕКСА
            CreateIndexRequest request = new CreateIndexRequest("books");
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 1)
            );
            ArrayList<String> prop = new ArrayList<>();
            prop.add(0,"author_id");
            prop.add(1,"author");
            prop.add(2,"publisher_id");
            prop.add(3,"book_id");
            prop.add(4,"title");
            prop.add(5,"description");
            prop.add(6,"series");
            prop.add(7,"year");
            prop.add(8,"number_of_pages");
            prop.add(9,"binding");
            prop.add(10,"interpreter");
            prop.add(11,"isbn");
            prop.add(12,"book_sizes");
            prop.add(13,"format");
            prop.add(14,"code");
            prop.add(15,"in_base");
            prop.add(16,"theme");
            prop.add(17,"circulation");
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_doc");
                {
                    builder.startObject("properties");
                    {
                        for (String i : prop){
                            builder.startObject(i);
                            {
                                builder.field("type", "text");
                            }
                            builder.endObject();
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping("_doc",builder);
            CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println("response id: "+indexResponse.index());
//ИНДЕКСИРОВАНИЕ ДОКУМЕНТОВ
            IndexRequest indexDocRequest = new IndexRequest("books");
            String jsonString = "{" +
                    "\"author_id\":\"\","+
                    "\"author\":\"\","+
                    "\"publisher_id\":\"\","+
                    "\"publisher\":\"\","+
                    "\"book_id\":\"\","+
                    "\"title\":\"\","+
                    "\"description\":\"\","+
                    "\"series\":\"\","+
                    "\"year\":\"\","+
                    "\"number_of_pages\":\"\","+
                    "\"binding\":\"\","+
                    "\"interpreter\":\"\","+
                    "\"isbn\":\"\","+
                    "\"book_sizes\":\"\","+
                    "\"format\":\"\","+
                    "\"code\":\"\","+
                    "\"in_base\":\"\","+
                    "\"theme\":\"\","+
                    "\"circulation\":\"\""+
                    "}";
            indexDocRequest.source(jsonString, XContentType.JSON);
            /*XContentBuilder contentBuilder = jsonBuilder();
            contentBuilder.startObject();
            {
                contentBuilder.startObject("properties");
                {
                    contentBuilder.startObject("message");
                    {
                        contentBuilder.field("author_id", "text");
                        contentBuilder.field("author", "text");
                        contentBuilder.field("publisher_id", "text");
                        contentBuilder.field("publisher", "text");
                        contentBuilder.field("book_id", "text");
                        contentBuilder.field("title", "text");
                        contentBuilder.field("description", "text");
                        contentBuilder.field("series", "text");
                        contentBuilder.field("year", "text");
                        contentBuilder.field("number_of_pages", "text");
                        contentBuilder.field("binding", "text");
                        contentBuilder.field("interpreter", "text");
                        contentBuilder.field("isbn", "text");
                        contentBuilder.field("book_sizes", "text");
                        contentBuilder.field("format", "text");
                        contentBuilder.field("code", "text");
                        contentBuilder.field("in_base", "text");
                        contentBuilder.field("theme", "text");
                        contentBuilder.field("circulation", "text");
                    }
                    contentBuilder.endObject();
                }
                contentBuilder.endObject();
            }
            contentBuilder.endObject();
            IndexRequest requestAddDoc = new IndexRequest("books");
//ПОЛУЧЕНИЕ ДАННЫХ
            GetRequest getRequest = new GetRequest(
                    "books",
                    "1");
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

            if (getResponse.isExists()) {
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString();
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();
                System.out.println(sourceAsString);
            } else {
                System.out.println("ERROR");
            }*/
//УДАЛЕНИЕ ИНДЕКСА
            /*DeleteIndexRequest request = new DeleteIndexRequest("books");
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);*/

//ОТКЛЮЧЕНИЕ КЛИЕНТА ELASTICSEARCH
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}

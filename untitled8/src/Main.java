import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import oracle.jdbc.*;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.sql.*;
import java.util.Locale;

public class Main {
    public static String ins(String s){
        return s.contains("\"")?s.replace("\"","\\\""):s;
    }

    public static void main(String[] args) {

//ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ
        Locale.setDefault(Locale.ENGLISH);
        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");
//СОЗДАНИЕ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ
            Connection con = DriverManager.getConnection("jdbc:oracle:thin:@user-PC:1521:xe", "usr", "q1234567");
//СОЗДАНИЕ ЗАПРОССА
            Statement stmnt1 = con.createStatement();
            Statement stmnt2 = con.createStatement();
            Statement stmnt3 = con.createStatement();
            ResultSet resultSetPublisher = stmnt1.executeQuery("select * from publisher " +
                    "inner join bk on publisher.publisher_id = bk.publisher_id");
            ResultSet resultSetAuthor = stmnt2.executeQuery("select * from author " +
                    "inner join bk on author.author_id=bk.author_id");
            ResultSet resultSetBook = stmnt3.executeQuery("select * from book " +
                    "inner join bk on book.book_id=bk.book_id");

//ИНДЕКСИРОВАНИЕ ДАННЫХ С ПОМОЩЬЮ ELASTICSEARCH

//ИЗВЛЕЧЕНИЕ ДАННЫХ КНИГ ИЗ БАЗЫ

                int i = 0;
                while (resultSetPublisher.next() && resultSetAuthor.next() && resultSetBook.next()) {
//ПОСТРОЕНИЕ ОБЪЕКТА
                   String author_id = resultSetAuthor.getString("author_id");

                    String author = resultSetAuthor.getString("author")==(null)?
                            "null":ins(resultSetAuthor.getString("author"));

                    String publisher_id = resultSetPublisher.getString("publisher_id");
                    String publisher = resultSetPublisher.getString("name_of_the_publisher")==(null)?
                            "null":ins(resultSetPublisher.getString("name_of_the_publisher"));
                    String book_id = resultSetBook.getString("book_id");
                    String title = resultSetBook.getString("title")==(null)?
                            "null":ins(resultSetBook.getString("title"));
                    String book_description = resultSetBook.getString("book_description")==(null)?
                            "null":ins(resultSetBook.getString("book_description"));
                    String series = resultSetBook.getString("series")==(null)?
                            "null":ins(resultSetBook.getString("series"));
                    String year = resultSetBook.getString("year")==(null)?
                            "null":ins(resultSetBook.getString("year"));
                    String pages = resultSetBook.getString("pages")==(null)?
                            "null":ins(resultSetBook.getString("pages"));
                    String binding = resultSetBook.getString("binding")==(null)?
                            "null":ins(resultSetBook.getString("binding"));
                    String interpreter = resultSetBook.getString("intepreter")==(null)?
                            "null":ins(resultSetBook.getString("intepreter"));
                    String isbn = resultSetBook.getString("isbn")==(null)?
                            "null":ins(resultSetBook.getString("isbn"));
                    String sizes_of_book = resultSetBook.getString("sizes_of_book")==(null)?
                            "null":ins(resultSetBook.getString("sizes_of_book"));
                    String format = resultSetBook.getString("format")==(null)?
                            "null":ins(resultSetBook.getString("format"));
                    String code = resultSetBook.getString("code")==(null)?
                            "null":ins(resultSetBook.getString("code"));
                    String in_base = resultSetBook.getString("in_base")==(null)?
                            "null":ins(resultSetBook.getString("in_base"));
                    String theme = resultSetBook.getString("theme")==(null)?
                            "null":ins(resultSetBook.getString("theme"));
                    String circulation = resultSetBook.getString("circulation")==(null)?
                            "null":ins(resultSetBook.getString("circulation"));
                    try {
//ПОДКЛЮЧЕНИЕ ES
                        RestHighLevelClient client = new RestHighLevelClient(
                                RestClient.builder(
                                        new HttpHost("localhost", 9200, "http"),
                                        new HttpHost("localhost", 9300, "http")));

                        IndexRequest indexDocRequest = new IndexRequest("books","_doc", String.valueOf(i));
                        String jsonString = "{" +
                                "\"author_id\":\""+author_id+"\","+
                                "\"author\":\""+author+"\","+
                                "\"publisher_id\":\""+publisher_id+"\","+
                                "\"publisher\":\""+publisher+"\","+
                                "\"book_id\":\""+book_id+"\","+
                                "\"title\":\""+title+"\","+
                                "\"description\":\""+book_description+"\","+
                                "\"series\":\""+series+"\","+
                                "\"year\":\""+year+"\","+
                                "\"number_of_pages\":\""+pages+"\","+
                                "\"binding\":\""+binding+"\","+
                                "\"interpreter\":\""+interpreter+"\","+
                                "\"isbn\":\""+isbn+"\","+
                                "\"book_sizes\":\""+sizes_of_book+"\","+
                                "\"format\":\""+format+"\","+
                                "\"code\":\""+code+"\","+
                                "\"in_base\":\""+in_base+"\","+
                                "\"theme\":\""+theme+"\","+
                                "\"circulation\":\""+circulation+"\""+
                                "}";
                        i++;
                        indexDocRequest.source(jsonString, XContentType.JSON);
                        IndexResponse indexResponse = client.index(indexDocRequest, RequestOptions.DEFAULT);

                        client.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

//СОЗДАНИЕ ИНДЕКСА ДЛЯ КНИГ
                }
//ЗАКРТЫИЕ ПОДКЛЮЧЕНИЯ БАЗЫ ДАННЫХ
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9300, "http")));
//ПОДКЛЮЧЕНИЕ ES

//ЗАПРОС: ЗДОРОВЬЕ КЛАСТЕРА
                        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
//ОТВЕТ: ЗДОРОВЬЕ КЛАСТЕРА
                        ClusterHealthResponse health = client.cluster().health(clusterHealthRequest, RequestOptions.DEFAULT);
                        System.out.println(health.toString());
            GetRequest getRequest = new GetRequest(
                    "books","1091");
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            } else {
                System.out.println("ERROR");
            }
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
//\
//\
//ИНДЕКСИРОВАНИЕ
//\\\\\\\\\\\\\\\
//ЗАПРОС НА СОЗДАНИЕ ИНДЕКСА ДЛЯ ИЗДАТЕЛЕЙ
                /*CreateIndexRequest createIndexRequestPublisher = new CreateIndexRequest("publisher");
//СОЗДАНИЕ ИНДЕКСА ДЛЯ ИЗДАТЕЛЕЙ
//КОЛИЧЕСТВО ШАРДОВ И РЕПЛИК
                createIndexRequestPublisher.settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
                );
//ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗДАТЕЛЕЙ ИЗ БАЗЫ
                ResultSet resultSetPublisher = stmnt.executeQuery("select * from publisher");
                while (resultSetPublisher.next()) {
//ПОСТРОЕНИЕ ОБЪЕКТА
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {
                    builder.field("publisher_id", resultSetPublisher.getString("publisher_id"));
                    builder.field("publisher", resultSetPublisher.getString("name_of_the_publisher"));
                }
                builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("publisher_id");
                            {
                                builder.field("type", "text");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    *//*createIndexRequestPublisher.mapping(
                            "{\n" +
                                    "  \"properties\": {\n" +
                                    "    \"publisher_id\": {\n" +
                                    "      \"type\": \"text\"\n" +
                                    "    }\n" +

                                    "  }\n" +
                                    "}",
                            XContentType.JSON);*//*
//СОЗДАНИЕ ИНДЕКСА ДЛЯ ИЗДАТЕЛЕЙ
               createIndexRequestPublisher.mapping("publisher",builder);
                }
//\
//ИНДЕКСИРОВАНИЕ
//АВТОРОВ
//ЗАПРОС НА СОЗДАНИЕ ИНДЕКСА ДЛЯ АВТОРОВ
                CreateIndexRequest createIndexRequestAuthor = new CreateIndexRequest("author");
//КОЛИЧЕСТВО ШАРДОВ И РЕПЛИК
                createIndexRequestAuthor.settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
                );
//ИЗВЛЕЧЕНИЕ ДАННЫХ АВТОРОВ ИЗ БАЗЫ
                ResultSet resultSetAuthor = stmnt.executeQuery("select * from author");
                while (resultSetAuthor.next()) {
//ПОСТРОЕНИЕ ОБЪЕКТА
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    {
                        builder.field("author_id", resultSetAuthor.getString("author_id"));
                        builder.field("author", resultSetAuthor.getString("author"));
                    }
                    builder.endObject();
//СОЗДАНИЕ ИНДЕКСА ДЛЯ АВТОРОВ
                    createIndexRequestAuthor.mapping(String.valueOf(builder));
                }
//\
//ИНДЕКСИРОВАНИЕ
//ТАБЛИЦЫ СВЯЗЕЙ
//ЗАПРОС НА СОЗДАНИЕ ИНДЕКСА
                CreateIndexRequest createIndexRequestBk = new CreateIndexRequest("bk");
//КОЛИЧЕСТВО ШАРДОВ И РЕПЛИК
                createIndexRequestBk.settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
                );
//ИЗВЛЕЧЕНИЕ ДАННЫХ ТАБЛИЦЫ СВЯЗЕЙ ИЗ БАЗЫ
                ResultSet resultSetBk = stmnt.executeQuery("select * from bk");
                while (resultSetBk.next()) {
//ПОСТРОЕНИЕ ОБЪЕКТА
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    {
                        builder.field("book_id", resultSetBk.getString("book_id"));
                        builder.field("author_id", resultSetBk.getString("author_id"));
                        builder.field("publisher_id", resultSetBk.getString("publisher_id"));
                    }
                    builder.endObject();
//СОЗДАНИЕ ИНДЕКСА ДЛЯ ТАБЛИЦЫ СВЯЗЕЙ
                    createIndexRequestBk.mapping(String.valueOf(builder));
                }/
//\
//ИНДЕКСИРОВАНИЕ
//КНИГ
//ЗАПРОС НА СОЗДАНИЕ ИНДЕКСА ДЛЯ КНИГ
                CreateIndexRequest createIndexRequestBook = new CreateIndexRequest("book");
//КОЛИЧЕСТВО ШАРДОВ И РЕПЛИК
                createIndexRequestBook.settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
                );*/
/*
//ПОЛУЧЕНИЕ ИНДЕКСА
                CreateIndexRequest request = new CreateIndexRequest("twitter");
                request.settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 1)
                );
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("message");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                request.mapping("1",builder);

                XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
                contentBuilder.startObject();
                {
                    contentBuilder.field("message","tweet");
                }
                contentBuilder.endObject();
                IndexRequest indexRequest = new IndexRequest("twitter").id("1").source(contentBuilder);


                GetRequest getIndexRequest = new GetRequest("twitter","1");
                GetResponse getResponse = client.get(getIndexRequest,RequestOptions.DEFAULT);
                System.out.println(getResponse.getField("message"));
                System.out.println(getIndexRequest.toString());

*/
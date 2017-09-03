import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;


/**
 * Created by Yixing Zhang on 7/5/17.
 */
public class LinkGraph {
  private String path;
  private String endPoint;
  private RestClient client;
  private JsonObject links;
  private JsonObject allDocnos;
  private JsonParser parser;
  private Gson gson;

  public LinkGraph(String path, String endPoint) {
    this.links = new JsonObject();
    this.path = path;
    this.endPoint = endPoint;
    this.parser = new JsonParser();
    this.client = RestClient
            .builder(new HttpHost("127.0.0.1", 9200, "http"))
            .setMaxRetryTimeoutMillis(100000)
            .build();
    this.gson = new GsonBuilder()
            .disableHtmlEscaping()
            .setPrettyPrinting()
            .create();
  }

  private void getAllDoc() {
    try {
      Response response = client.performRequest("GET",
              endPoint + "_search?_source=false&size=60000",
              Collections.emptyMap());
      allDocnos = parser.parse(new InputStreamReader(
              response.getEntity().getContent(), StandardCharsets.UTF_8))
              .getAsJsonObject();

      System.out.println("Load all docs took: " + allDocnos.get("took"));
      System.out.println("Total documents: " + allDocnos.getAsJsonObject("hits")
              .get("total"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void go() throws Exception {
    getAllDoc();
    loadLinks();
    outputLinks();
    close();
  }

  private void loadLinks() {
    JsonArray hits = allDocnos
            .getAsJsonObject("hits")
            .get("hits")
            .getAsJsonArray();
    int count = 0;

    for (JsonElement hit : hits) {
      String docno = hit.getAsJsonObject().get("_id").getAsString();
      JsonObject jo = getDoc(docno);
      JsonArray inLinks = jo.getAsJsonArray("in_links");
      JsonArray outLinks = jo.getAsJsonArray("out_links");
      JsonObject newLinks = new JsonObject();
      newLinks.add("in_links", inLinks);
      newLinks.add("out_links", outLinks);
      links.add(docno, newLinks);
      count++;
      if (count % 1000 == 0) {
        System.out.println(count + " documents added.");
      }
    }
    System.out.println("Finish adding: " + count + " documents.");
  }

  private void outputLinks() throws Exception {
    FileUtils.writeStringToFile(new File(path + "linkGraph.txt"),
            gson.toJson(links),
            "UTF-8");
  }

  private JsonObject getDoc(String docno) {
    try {
      Response response = client.performRequest("GET",
              endPoint + docno,
              Collections.emptyMap());
      JsonObject jsonObject = parser.parse(new InputStreamReader(
              response.getEntity().getContent(), StandardCharsets.UTF_8))
              .getAsJsonObject().getAsJsonObject("_source");
      return jsonObject;
    }
    catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private void close() {
    try {
      client.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    LinkGraph linkGraph = new LinkGraph("/Users/tommy/CS/CS6200/hw3/",
            "/test/document/");
    linkGraph.go();
  }
}

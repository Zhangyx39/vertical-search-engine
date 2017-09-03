import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by Yixing Zhang on 6/27/17.
 */

public class UpLoader {
  String path;
  String endPoint;
  JsonObject links;
  RestClient restClient;
  JsonParser parser;

  int count;
  int merged;

  public UpLoader(String path, String endPoint) throws Exception {
    this.path = path;
    this.endPoint = endPoint;
    this.parser = new JsonParser();
    loadLinks();
    this.restClient = RestClient
            .builder(new HttpHost("127.0.0.1", 9200, "http"))
            .setMaxRetryTimeoutMillis(5000)
            .build();

    count = 0;
    merged = 0;
  }

  private void loadLinks() throws Exception {
    File linkstxt = new File(path + "output/links.txt");
    links = parser.parse(new FileReader(linkstxt)).getAsJsonObject();
    System.out.println("Links loaded successfully.");
  }

  private void upLoadDoc(JsonObject doc) {

    String docno = doc.get("docno").getAsString();
    JsonObject allLinks = links.getAsJsonObject(docno);
    JsonArray outlinks;
    JsonArray inlinks;

    if (docno.length() > 500) {
      docno = docno.substring(0, 500);
    }

    if (allLinks == null) {
      outlinks = new JsonArray();
      inlinks = new JsonArray();
    } else {
      outlinks = allLinks.getAsJsonArray("out_links");
      inlinks = allLinks.getAsJsonArray("in_links");
    }

    JsonObject oldDoc = getDoc(docno);
    if (oldDoc == null) {

      JsonObject oldHeader = doc.getAsJsonObject("HTTPheader");
      JsonObject newHeader = new JsonObject();
      ArrayList<String> fields = new ArrayList<>(Arrays.asList(
              "Server", "Last-Modified", "Date",
              "Content-Type", "Content-Length"));
      for (String field : fields) {
        if (oldHeader.has(field)) {
          newHeader.add(field, oldHeader.get(field));
        }
      }

      doc.add("HTTPheader", newHeader);
      doc.add("out_links", outlinks);
      doc.add("in_links", inlinks);
      putDoc(doc, docno);
    } else {
      JsonObject newDoc = new JsonObject();
      try {
        newDoc.addProperty("author",
                oldDoc.get("author").getAsString() + " Yixing");
      } catch (Exception e) {
        newDoc.addProperty("author", "Yixing");
      }

      try {
        JsonArray oldInlinks = oldDoc.getAsJsonArray("in_links");
        for (JsonElement link : oldInlinks) {
          if (!inlinks.contains(link)) {
            inlinks.add(link);
          }
        }
      } catch (Exception e) {
      }
      try {
        JsonArray oldOutlinks = oldDoc.getAsJsonArray("out_links");
        for (JsonElement link : oldOutlinks) {
          if (!outlinks.contains(link)) {
            outlinks.add(link);
          }
        }
      } catch (Exception e) {
      }

      newDoc.add("in_links", inlinks);
      newDoc.add("out_links", outlinks);
      updateDoc(newDoc, docno);
    }

  }

  private void updateDoc(JsonObject jo, String id) {
    JsonObject wrapper = new JsonObject();
    wrapper.add("doc", jo);

    HttpEntity httpEntity = new NStringEntity(wrapper.toString(),
            ContentType.APPLICATION_JSON);

    try {
      restClient.performRequest("POST",
              endPoint + id + "/_update",
              Collections.emptyMap(),
              httpEntity);
      count++;
      merged++;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("fail to update: " + id);
    }
  }

  private void putDoc(JsonObject jo, String id) {
    HttpEntity httpEntity = new NStringEntity(jo.toString(),
            ContentType.APPLICATION_JSON);

    try {
      restClient.performRequest("PUT",
              endPoint + id,
              Collections.emptyMap(),
              httpEntity);
      count++;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("fail to upload: " + id);
    }
  }

  private JsonObject getDoc(String docno) {
    try {
      Response response = restClient.performRequest("GET",
              endPoint + docno,
              Collections.emptyMap());
      JsonObject jo = parser.parse(new InputStreamReader(
              response.getEntity().getContent(), StandardCharsets.UTF_8))
              .getAsJsonObject().getAsJsonObject("_source");
      return jo;
    } catch (Exception e) {
      return null;
    }
  }

  private void upLoadFile(File file) throws Exception {
    JsonArray docs = parser.parse(new FileReader(file)).getAsJsonArray();
    for (JsonElement doc : docs) {
      upLoadDoc(doc.getAsJsonObject());
    }
  }

  public void upLoadAll() throws Exception {
    long start = System.currentTimeMillis();
    File allFiles = new File(path + "output/result");
    File[] list = allFiles.listFiles();
    for (File file : list) {
      System.out.println("uploading: " + file.toString());
      upLoadFile(file);
    }
    long end = System.currentTimeMillis();
    System.out.println("Time used: " + Crawler.computeTime(end - start));
  }

  public void close() throws Exception {
    System.out.println("Total uploaded files: " + count);
    System.out.println("Merged: " + merged);
    restClient.close();
  }
  
  public static void main(String[] args) throws Exception {
    String path = "/Users/tommy/CS/CS6200/hw4/";
    UpLoader upLoader = new UpLoader(path,
            "/demo/document/");

    upLoader.upLoadAll();
    upLoader.close();

  }
}

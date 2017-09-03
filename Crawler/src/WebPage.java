import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import our.canonicalizer.Canonicalizer;

/**
 * Created by Yixing Zhang on 6/21/17.
 */
public class WebPage {

  @Expose
  @SerializedName("depth")
  private int wave;
  private boolean crawled;
  private boolean success;
  private int score;

  @Expose
  @SerializedName("url")
  private String url;
  private String protocol;
  private String domain;
  private String path;
  @Expose
  @SerializedName("title")
  private String title;
  @Expose
  @SerializedName("docno")
  private String canonicalizedURL;
  @Expose
  @SerializedName("HTTPheader")
  private Map<String, String> header;
  @Expose
  @SerializedName("text")
  private String text;
  @Expose
  @SerializedName("html_Source")
  private String htmlSource;
  @Expose
  @SerializedName("author")
  private String author;

  private HashMap<String, WebPage> inLinks;
  private HashMap<String, WebPage> outLinks;

  public WebPage(String url, int wave) throws Exception {
    this.wave = wave;
    this.crawled = false;
    this.success = false;
    this.score = 0;
    this.url = url;

    URI uri = new URI(url);
    this.protocol = uri.getScheme().toLowerCase();
    this.domain = uri.getHost().toLowerCase();
    this.path = uri.getPath();

    this.title = "";
    this.canonicalizedURL = Canonicalizer.apply(url);
    this.header = new HashMap<>();
    this.text = "";
    this.htmlSource = "";
    this.author = "Yixing";
    this.inLinks = new HashMap<>();
    this.outLinks = new HashMap<>();
  }


  public List<String[]> analyze() throws Exception {
    this.crawled = true;
    Connection connection = Jsoup
            .connect(URLDecoder.decode(url, "UTF-8"))
//            .validateTLSCertificates(false)
            .timeout(3000);

    Connection.Response response = connection.execute();
//    if (!(response.statusCode() == 200)) {
//      throw new Exception(response.statusMessage());
//    }

    Document d = response.parse();

    this.header.putAll(response.headers());
    this.title = d.title();
    this.htmlSource = d.html();
    Element e = d.body();
    this.text = e == null ? "" : e.text();

    List<String[]> toReturn = allOutLinks(d);
    this.success = true;
    return toReturn;
  }

  private List<String[]> allOutLinks(Document d) {
    ArrayList<String[]> urls = new ArrayList<>();

    for (Element a : d.getElementsByTag("a")) {

      String text = a.text() + " " + a.attr("title");
      String url = a.attr("href").replaceAll(" ", "");

      if (url.equals("") || stopList(url)) {
        continue;
      }
      if (url.startsWith("//")) {
        url = protocol + ":" + url;
      } else if (url.startsWith("/")) {
        url = protocol + "://" + domain + url;
      } else if (!url.toLowerCase().startsWith("http")) {
        String cutPath = path.substring(0, path.lastIndexOf("/") + 1);
        url = protocol + "://" + domain + cutPath + url;
      }

      String[] link = new String[2];
      link[0] = text.replaceAll("[^A-Za-z0-9 ]", " ").toLowerCase();
      link[1] = url;
      urls.add(link);

    }
    return urls;
  }

  private boolean stopList(String url) {
    return url.startsWith("#")
            || url.startsWith("javascript")
            || url.endsWith(".jpg")
            || url.endsWith(".png")
            || url.endsWith(".gif")
            || url.endsWith(".pdf")
            || url.endsWith(".php");

  }

  public void clearTextAndHtml() {
    this.text = "";
    this.htmlSource = "";
  }

  public String toJson() {
    Gson gson = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .create();

//    JsonObject jo = gson.toJsonTree(this).getAsJsonObject();
//    JsonArray in = new JsonArray();
//    for (WebPage webPage : inLinks.values()) {
//      in.add(webPage.getUrl());
//    }
//    JsonArray out = new JsonArray();
//    for (WebPage webPage : outLinks.values()) {
//      out.add(webPage.getUrl());
//    }
//
//    jo.add("in_links", in);
//    jo.add("out_links", out);
//    return gson.toJson(jo);
    return gson.toJson(this);
  }

  public void addInLink(WebPage w) {
    inLinks.put(w.getCanonicalizedURL(), w);
  }

  public void addOutLink(WebPage w) {
    outLinks.put(w.getCanonicalizedURL(), w);
  }

  public void setCrawled(boolean crawled) {
    this.crawled = crawled;
  }

  public void setScore(int score) {
    this.score = score;
  }

  public boolean isCrawled() {
    return crawled;
  }


  public boolean isSuccess() {
    return success;
  }

  public int getScore() {
    return score;
  }

  public String getDomain() {
    return domain;
  }

  public int getWave() {
    return wave;
  }

  public String getUrl() {
    return url;
  }

  public String getTitle() {
    return title;
  }

  public String getCanonicalizedURL() {
    return canonicalizedURL;
  }

  public HashMap<String, WebPage> getInLinks() {
    return inLinks;
  }

  public HashMap<String, WebPage> getOutLinks() {
    return outLinks;
  }

  public static void main(String[] args) throws Exception {
    String url1 = "http://en.wikipedia.org/wiki/Climate_change";
    String url2 = "https://www.google.com/search?client=safari&rls=en&q=CLIMATE+CHANGE&ie=UTF-8&oe=UTF-8#q=CLIMATE+CHANGE&safe=off&rls=en&tbm=nws";
    String url3 = "http://www.nasa.gov/mission_pages/noaa-n/climate/climate_weather.html#.VP9bRyklBYw";
    String url4 = "https://nsidc.org/cryosphere/arctic-meteorology/climate_vs_weather.html";
    String url5 = "http://en.wikipedia.org/wiki/Weather_and_climate";
    String url6 = "https://www.google.com/search?client=safari&rls=en&q=difference+between+weather+and+climate&ie=UTF-8&oe=UTF-8";
    String url7 = "http://www.noaa.gov/explainers/what-s-difference-between-climate-and-weather";
    String url8 = "http://www.naaa" +
            ".gov/explainers/what-s-difference-between-climate-and-weather";
    String url9 = "http:///";
//    WebPage wp1 = new WebPage(url8, 0);
//    wp1.analyze();

    Jsoup.connect(url9).get();
//    System.out.println(wp1.wave);
//    System.out.println(wp1.url);
//    System.out.println(wp1.domain);
//    System.out.println(wp1.title);
//    System.out.println(wp1.canonicalizedURL);
//    System.out.println(wp1.header);
//    System.out.println(wp1.htmlSource);
//    System.out.println(wp1.text);

//    System.out.println(wp1.toJson());

  }
}

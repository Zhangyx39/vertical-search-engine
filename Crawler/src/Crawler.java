import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;


/**
 * Created by Yixing Zhang on 6/21/17.
 */
public class Crawler {
  private PriorityQueue<WebPage> queue;
  private HashMap<String, WebPage> webPages;
  private HashMap<String, Domain> domains;
  private ArrayList<String> keyWords;
  private StringBuilder log;

  private int count;
  private long totalSleep;
  private int num;
  private StringBuilder sb;

  private String outPath = "/Users/tommy/CS/CS6200/hw4/";

  public Crawler() {
    this.webPages = new HashMap<>();
    this.domains = new HashMap<>();
    this.keyWords = new ArrayList<>();
    this.log = new StringBuilder();

    this.count = 0;
    this.totalSleep = 0;
    this.num = 0;
    this.sb = new StringBuilder();

    this.queue = new PriorityQueue<>(new Comparator<WebPage>() {
      @Override
      public int compare(WebPage o1, WebPage o2) {
        if (o1.getWave() == o2.getWave()) {
          return Integer.compare(o2.getScore(), o1.getScore());
        } else {
          return o1.getWave() - o2.getWave();
        }
      }
    });
  }

  public void addSeed(List<String> urls) throws Exception {
    for (String url : urls) {
      WebPage w = new WebPage(url, 0);
      queue.add(w);
    }
  }

  public void addWaveOne(List<String> urls) throws Exception {
    for (String url : urls) {
      WebPage w = new WebPage(url, 1);
      queue.add(w);
    }
  }

  public void setKeyWords(List<String> strings) {
    this.keyWords.addAll(strings);
  }

  public void crawl(int num) {
    this.num = num;
    System.out.println("Crawler starts.");
    System.out.println("Keywords: " + keyWords);
    log.append("Crawler starts.\n");
    log.append("Keywords: " + keyWords + "\n");

    long startTime = System.currentTimeMillis();
    count = 0;
    while (!queue.isEmpty() && count < num) {

      WebPage currentPage = queue.remove();
      try {
        // print out information
        System.out.println();
//        System.out.println("wave: " + currentPage.getWave());
        System.out.println("queue size: " + queue.size());
        log.append("\n" + "wave: " + currentPage.getWave() + "\n");
        log.append("queue size: " + queue.size() + "\n");


        // add this web page to hash map
        String canonURL = currentPage.getCanonicalizedURL();
        if (!webPages.containsKey(canonURL)) {
          webPages.put(canonURL, currentPage);
        } else if (webPages.get(canonURL).isCrawled()) {
          // if it has been crawled, skip it.
          continue;
        }

        // sleep at most 1s if needed to be polite
        if (!sleep(currentPage)) {
          currentPage.setCrawled(true);
          System.out.println("Disallow: " + currentPage.getUrl());
          log.append("Disallow: " + currentPage.getUrl() + "\n");
          continue;
        }

        long currentTime = System.currentTimeMillis();
        List<String[]> outLinks = currentPage.analyze();
        domains.get(currentPage.getDomain()).setLastVisit(currentTime);
        // if url can be connected, count++.
        count++;

        System.out.println("Crawling the " + count + " page:");
        System.out.println(currentPage.getTitle() + ": " + currentPage.getScore());
//        System.out.println(currentPage.getUrl());
//        System.out.println("out links: " + outLinks.size());
        log.append("Crawling the " + count + " page:" + "\n");
        log.append(currentPage.getTitle() + ": " + currentPage.getScore() +
                "\n");
        log.append(currentPage.getUrl() + "\n");
        log.append("out links: " + outLinks.size() + "\n");

        int newLinks = 0;

        for (String[] outLink : outLinks) {
          // check if the url is valid
          try {
            int score = computeScore(outLink[0]);

            // if score is low, skip it.
            if (score < 2) {
              continue;
            }

            WebPage newPage = new WebPage(outLink[1], currentPage.getWave() + 1);
            if (!webPages.containsKey(newPage.getCanonicalizedURL())) {

              // if it is a new page, add to the hash map and queue
              webPages.put(newPage.getCanonicalizedURL(), newPage);
              newPage.setScore(currentPage.getScore() + score);
              queue.add(newPage);
              newLinks++;

            } else {
              // this is not a new page, find it in hash map
              newPage = webPages.get(newPage.getCanonicalizedURL());
            }

            // connect the pages
            currentPage.addOutLink(newPage);
            newPage.addInLink(currentPage);
          } catch (Exception e) {
            System.out.println("********");
            System.out.println(outLink[0] + " " + outLink[1]);
            System.out.println(e);
            log.append("********\n");
            log.append(outLink[0] + " " + outLink[1] + "\n");
            log.append(e + "\n");
          }
        }
//        System.out.println(newLinks + " new links added to queue.");
        log.append(newLinks + " new links added to queue.\n");

        output(currentPage);
        currentPage.clearTextAndHtml();

      } catch (Exception e) {
        System.out.println(e);
        log.append(e);
        System.out.println("Cannot connect to: " + currentPage.getUrl());
        log.append("Cannot connect to: " + currentPage.getUrl() + "\n");
      }
    }

    // finish
    long currentTime = System.currentTimeMillis();
    System.out.println();
    System.out.println("Finish crawling " + count + " pages.");
    System.out.println("Total time spent: "
            + computeTime(currentTime - startTime));
    System.out.printf("Total sleep time: " + computeTime(totalSleep));

    log.append("\n" + "Finish crawling " + count + " pages.\n");
    log.append("Total time spent: "
            + computeTime(currentTime - startTime) + "\n");
    log.append("Total sleep time: " + computeTime(totalSleep) + "\n");

    outputLinks();

    try {
      DateFormat dateFormat = new SimpleDateFormat("MM_dd_HH:mm:ss");
      Date date = new Date();
      FileUtils.writeStringToFile(new File(outPath +
                      "output/logs/" + dateFormat.format(date) + ".txt"),
              log.toString(),
              "UTF-8");
    } catch (Exception e) {

    }
  }

  private void output(WebPage webPage) {
    int max = 100;
    int fileNo = (count - 1) / max;
    int docno = (count - 1) % max + 1;
    if (docno == 1) {
      sb = new StringBuilder();
      sb.append("[");
      sb.append(webPage.toJson());
    } else if (docno == max || count == num) {
      sb.append(",\n");
      sb.append(webPage.toJson());
      sb.append("]");
      String fileName = outPath + "output/result/" + fileNo + ".txt";
      try {
        FileUtils.writeStringToFile(new File(fileName),
                sb.toString(), "UTF-8");
        System.out.println("output: " + fileName);
        log.append("output: " + fileName + "\n");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      sb.append(",\n");
      sb.append(webPage.toJson());
    }
  }

  public void outputLinks() {
    Gson gson = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .create();

    JsonObject jo = new JsonObject();
    int count = 0;

    for (WebPage webPage : webPages.values()) {
      if (webPage.isSuccess()) {
        JsonObject web = new JsonObject();

        JsonArray olinks = new JsonArray();
        for (WebPage page : webPage.getOutLinks().values()) {
          olinks.add(page.getCanonicalizedURL());
        }
        web.add("out_links", olinks);

        JsonArray ilinks = new JsonArray();
        for (WebPage page : webPage.getInLinks().values()) {
          ilinks.add(page.getCanonicalizedURL());
        }
        web.add("in_links", ilinks);

        jo.add(webPage.getCanonicalizedURL(), web);
        count++;
      }
    }
    log.append("Output links: " + count + "\n");
    try {
      FileUtils.writeStringToFile(new File(outPath + "output/links.txt"),
              gson.toJson(jo), "UTF-8");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String computeTime(long timeInMs) {
    long timeInS = timeInMs / 1000;
    long hour = timeInS / 3600;
    long min = timeInS / 60 % 60;
    long sec = timeInS % 60;
    return hour + "h " + min + "min " + sec + "s.";
  }

  private int computeScore(String input) {
    int score = 0;

    for (String keyWord : keyWords) {
      if (input.matches("(.*)" + keyWord + "(.*)")) {
        score++;
      }
    }
    return score;
  }

  private boolean sleep(WebPage currentPage) throws InterruptedException {
    long currentTime = System.currentTimeMillis();
    String url = currentPage.getUrl();
    String domain = currentPage.getDomain();

    if (!domains.containsKey(domain)) {
      Domain newDomain = new Domain(domain);
      domains.put(domain, newDomain);
      System.out.println("new domain: " + domain);
      log.append("new domain: " + domain + "\n");
    }

    if (!domains.get(domain).isAllow(url)) {
      return false;
    }
    else {
      long diff = currentTime - domains.get(domain).getLastVisit();
      long delay = domains.get(domain).getDelay();
      if (diff < delay) {
        System.out.println("sleep: " + (delay - diff) + "ms");
        Thread.sleep(delay - diff);
        totalSleep += delay - diff;
      }
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    String url1 = "http://en.wikipedia.org/wiki/Climate_change";
    String url2 = "https://www.google.com/search?client=safari&rls=en&q=CLIMATE+CHANGE&ie=UTF-8&oe=UTF-8#q=CLIMATE+CHANGE&safe=off&rls=en&tbm=nws";
//    String url3 = "http://www.nasa.gov/mission_pages/noaa-n/climate/climate_weather.html";
    String url4 = "https://nsidc.org/cryosphere/arctic-meteorology/climate_vs_weather.html";
    String url5 = "http://en.wikipedia.org/wiki/Weather_and_climate";
    String url6 = "https://www.google.com/search?client=safari&rls=en&q=difference+between+weather+and+climate&ie=UTF-8&oe=UTF-8";

//    String url7 = "http://www.noaa.gov/explainers/what-s-difference-between-climate-and-weather";
    String url8 = "http://www.geography.learnontheinternet.co.uk/topics/weather.html";
    String url9 = "http://oceanservice.noaa.gov/facts/weather_climate.html";
    String url10 = "https://nsidc.org/cryosphere/arctic-meteorology/climate_vs_weather.html";
    String url11 = "https://www.americangeosciences.org/critical-issues/faq/difference-between-weather-and-climate";
    String url12 = "https://www.britannica.com/demystified/whats-the-difference-between-weather-and-climate";
    String url13 = "https://www.ucar.edu/learn/1_2_2_8t.htm";
    String url14 = "http://www.diffen.com/difference/Climate_vs_Weather";
    String url15 = "http://mentalfloss.com/article/501658/whats-difference-between-weather-and-climate";
    String url16 = "http://www.differencebetween.info/difference-between-weather-and-climate";


    ArrayList<String> seed = new ArrayList<>();
    seed.add(url1);
    seed.add(url2);
//    seed.add(url3);
    seed.add(url4);
    seed.add(url5);
    seed.add(url6);
//    seed.add(url7);

    ArrayList<String> waveOne = new ArrayList<>();
    waveOne.add(url8);
    waveOne.add(url9);
    waveOne.add(url10);
    waveOne.add(url11);
    waveOne.add(url12);
    waveOne.add(url13);
    waveOne.add(url14);
    waveOne.add(url15);
    waveOne.add(url16);

    String kw1 = "climate";
    String kw2 = "chang";
    String kw3 = "differ";
    String kw4 = "weather";
    ArrayList<String> keyWords = new ArrayList<>();
    keyWords.add(kw1);
    keyWords.add(kw2);
    keyWords.add(kw3);
    keyWords.add(kw4);


    try {
      Crawler crawler = new Crawler();
      crawler.addSeed(seed);
      crawler.addWaveOne(waveOne);
      crawler.setKeyWords(keyWords);
      crawler.crawl(3);
    } catch (OutOfMemoryError error) {
      error.fillInStackTrace();
      System.out.println(Runtime.getRuntime().totalMemory());
      System.out.println(Runtime.getRuntime().maxMemory());
      System.out.println(Runtime.getRuntime().freeMemory());
      DateFormat dateFormat = new SimpleDateFormat("MM_dd_HH:mm:ss");
      Date date = new Date();
      System.out.println(dateFormat.format(date));
    }

  }
}

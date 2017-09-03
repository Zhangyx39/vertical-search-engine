import org.apache.http.entity.BufferedHttpEntity;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.Scanner;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by Yixing Zhang on 6/24/17.
 */
public class Domain {
  private long lastVisit;
  private String domain;
  private BaseRobotRules rules;
  private long delay;

  public Domain(String domain) {
    this.lastVisit = 0;
    this.domain = domain;
    this.delay = 1000;
    getRobot();
  }

  public void setLastVisit(long lastVisit) {
    this.lastVisit = lastVisit;
  }

  public long getLastVisit() {

    return lastVisit;
  }

  public long getDelay() {
    return delay;
  }

  private void getRobot() {
    try {
      URL url = new URL("http://" + domain + "/robots.txt");

      Connection connection = Jsoup
              .connect(URLDecoder.decode(url.toString(), "UTF-8"))
//            .validateTLSCertificates(false)
              .timeout(1000);

      Connection.Response response = connection.execute();

      SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
      this.rules = robotParser.parseContent("http://domain.com",
              response.bodyAsBytes(),
              "text/plain",
              "yixing_crawler");

      this.delay = Math.max(rules.getCrawlDelay() * 1000, delay);
    }
    catch (Exception e) {
      rules = new SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL);
    }
  }

  public boolean isAllow(String url) {
    return rules.isAllowed(url);
  }
}

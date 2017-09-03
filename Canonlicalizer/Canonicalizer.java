package our.canonicalizer;

import java.net.URI;
import sentric.URL;

public class Canonicalizer {

  public static String apply(String url) throws Exception {
    URI uri = new URI(url);
    URL url1 = new URL(uri.normalize().toString());
    String toReturn = url1
            .getNormalizedUrl()
            .replaceAll("((ved)|(sa)|(client)|(rls))+(=)+[^&]*+(&*)", "")
            .replaceAll("[^A-Za-z0-9]","");
    return toReturn;
  }
}

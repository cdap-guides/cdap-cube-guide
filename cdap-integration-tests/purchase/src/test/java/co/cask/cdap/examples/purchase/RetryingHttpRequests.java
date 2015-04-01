package co.cask.cdap.examples.purchase;

import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;

import java.io.IOException;

/**
 *
 */
public class RetryingHttpRequests {

  public static HttpResponse execute(HttpRequest request) throws IOException {
    int maxRetries = 10;

    int retry = 0;
    HttpResponse response;
    while (true) {
      response = HttpRequests.execute(request);
      if (response.getResponseCode() != 503 || retry >= maxRetries) {
        break;
      }

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        break;
      }

      retry++;
    }

    return response;
  }
}

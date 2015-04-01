package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class PurchaseTest extends IntegrationTestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    ApplicationManager app = deployApplication(PurchaseApp.class);
    FlowManager flow = app.startFlow("PurchaseFlow");

    StreamWriter stream = app.getStreamWriter("purchaseStream");
    stream.send("Joe bought 3 dogs for $83");
    stream.send("Bob bought 8 pineapples for $1");
    stream.send("Joe bought 9 cats for $382");

    getProgramClient().waitForStatus(PurchaseApp.APP_NAME, ProgramType.FLOW, "PurchaseFlow",
                                     "RUNNING", 30, TimeUnit.SECONDS);

    RuntimeMetrics collectorMetrics = flow.getFlowletMetrics("collector");
    collectorMetrics.waitFor("system.process.events.processed", 3, 15, TimeUnit.SECONDS);

    MapReduceManager mapreduce = app.startMapReduce("PurchaseHistoryBuilder");
    mapreduce.waitForFinish(2, TimeUnit.MINUTES);

    ServiceManager service = app.startService("PurchaseHistoryService");
    URL serviceURL = service.getServiceURL(30, TimeUnit.SECONDS);
    URL requestURL = new URL(serviceURL.toString() + "/history/Joe");
    HttpRequest request = HttpRequest.get(requestURL).build();

    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    PurchaseHistory history = GSON.fromJson(response.getResponseBodyAsString(), PurchaseHistory.class);

    Assert.assertEquals("Joe", history.getCustomer());
    Assert.assertEquals(2, history.getPurchases().size());

    Purchase dogs;
    Purchase cats;
    if ("dog".equals(history.getPurchases().get(0).getProduct())) {
      dogs = history.getPurchases().get(0);
      cats = history.getPurchases().get(1);
    } else {
      dogs = history.getPurchases().get(1);
      cats = history.getPurchases().get(0);
    }

    Assert.assertEquals("dog", dogs.getProduct());
    Assert.assertEquals(3, dogs.getQuantity());
    Assert.assertEquals(83, dogs.getPrice());

    Assert.assertEquals("cat", cats.getProduct());
    Assert.assertEquals(9, cats.getQuantity());
    Assert.assertEquals(382, cats.getPrice());

    flow.stop();
  }

}

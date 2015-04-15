package co.cask.cdap.examples.longrunner;

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;

/**
 * Long running flow
 */
public class LongRunningFlow implements Flow {

  @Override
  public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("LongRunningFlow")
        .setDescription("Long running flow")
        .withFlowlets()
        .add("flowlet1", new MetricEmitterFlowlet())
        .add("flowlet2", new MetricEmitterFlowlet())
        .add("flowlet3", new MetricEmitterFlowlet())
        .add("flowlet4", new FourthFlowlet())
        .connect()
        .fromStream("eventStream").to("flowlet1")
        .from("flowlet1").to("flowlet2")
        .from("flowlet2").to("flowlet3")
        .from("flowlet3").to("flowlet4")
        .build();
  }
}

package com.redhat.riskvalidationservice.routes;

import com.google.gson.Gson;
import com.redhat.riskvalidationservice.beans.CollectEventsStrategy;
import com.redhat.riskvalidationservice.beans.FilterStrategy;
import com.redhat.riskvalidationservice.beans.RiskValidationBean;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;

import java.util.logging.Logger;

public class RiskValidationRouteBuilder extends RouteBuilder {

	private static final Logger LOG = Logger.getLogger(RiskValidationRouteBuilder.class.getName());

	private String kafkaBootstrap = "my-cluster-kafka-brokers:9092";
	private String kafkaCreditTransferCreditorTopic = "sensu";
	private String consumerMaxPollRecords ="50000";
	private String consumerCount = "3";
	private String consumerSeekTo = "beginning";
	private String consumerGroup = "invokeansible";
	private String consumerGroup2 = "risk";




	@Override
	public void configure() throws Exception {
		try {
			System.out.println("starting account validation service");


			KafkaComponent kafka = new KafkaComponent();
			kafka.setBrokers(kafkaBootstrap);
			this.getContext().addComponent("kafka", kafka);

			from("kafka:" + "statuschck" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
					+ consumerMaxPollRecords + "&seekTo=" + "end"
					+ "&groupId=" + consumerGroup).id("apbEvents")

			.bean(RiskValidationBean.class,"prepareAnsibleRequest")
			.log("${body}")
					.to("kafka:"+"crtincident"+ "?brokers=" + kafkaBootstrap);

			from("kafka:" + "crtincident" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
					+ consumerMaxPollRecords + "&seekTo=" + "end"
					+ "&groupId=" + consumerGroup)
					.bean(RiskValidationBean.class,"createIncident")
					.log("${body}")
					.setHeader(Exchange.HTTP_METHOD, constant("POST"))
					.setHeader("Authorization",constant("Basic YWRtaW5Vc2VyOlJlZEhhdA=="))
					.setHeader("Content-Type",constant("application/json"))
					.choice()
					.when(body().isNotNull())
					.toD("http://rhpam-trial-kieserver-http-self-healing.apps.cluster-fc8n8.fc8n8.sandbox93.opentlc.com/services/rest/server/containers/Workflows_1.0.0-SNAPSHOT/processes/Workflows.FailedPlaybook/instances")
			        .to("kafka:"+"inc"+"?brokers=" + kafkaBootstrap)
					.log("${body}")
					.otherwise()
			.log("skipped incident creation");


			from("kafka:" + "failed-decision" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
					+ consumerMaxPollRecords + "&seekTo=" + "end"
					+ "&groupId=" + consumerGroup)
					.bean(RiskValidationBean.class,"createIncident")
					.log("${body}")
					.setHeader(Exchange.HTTP_METHOD, constant("POST"))
					.setHeader("Authorization",constant("Basic YWRtaW5Vc2VyOlJlZEhhdA=="))
					.setHeader("Content-Type",constant("application/json"))
					.toD("http://rhpam-trial-kieserver-http-self-healing.apps.cluster-fc8n8.fc8n8.sandbox93.opentlc.com/services/rest/server/containers/Workflows_1.0.0-SNAPSHOT/processes/Workflows.FailedEvent/instances")
			        .log("${body}")
					.to("kafka:"+"inc"+"?brokers=" + kafkaBootstrap)
			.log("${body}")

					;








		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}

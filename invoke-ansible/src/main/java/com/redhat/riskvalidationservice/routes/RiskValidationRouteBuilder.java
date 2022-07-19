package com.redhat.riskvalidationservice.routes;

import com.google.gson.Gson;
import com.redhat.riskvalidationservice.beans.CollectEventsStrategy;
import com.redhat.riskvalidationservice.beans.FilterStrategy;
import com.redhat.riskvalidationservice.beans.RiskValidationBean;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.springframework.beans.factory.annotation.Value;

import org.apache.camel.component.http4.HttpComponent;
import org.apache.camel.util.jsse.SSLContextParameters;
import org.apache.camel.util.jsse.TrustManagersParameters;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;

import javax.enterprise.inject.Produces;
import javax.inject.Named;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


import java.util.logging.Logger;

public class RiskValidationRouteBuilder extends RouteBuilder {


	@Value("${ansible.tower.url}")
	String ansibleTowerUrl;



	private static final Logger LOG = Logger.getLogger(RiskValidationRouteBuilder.class.getName());

	private String kafkaBootstrap = "my-cluster-kafka-brokers:9092";
//private String kafkaBootstrap = "localhost:9092";
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

			HttpComponent httpComponent = createCustomHttp4Component();
			this.getContext().addComponent("https4", httpComponent);


			from("kafka:" + "event-decision" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
					+ consumerMaxPollRecords + "&seekTo=" + "beginning"
					+ "&groupId=" + consumerGroup).id("apbEvents")
			.bean(RiskValidationBean.class,"prepareAnsibleRequest")
					.setHeader(Exchange.HTTP_METHOD, constant("POST"))
					.setHeader("Authorization",constant("Bearer  Cl5Ffl1ieyxcs00sWbcjST13gfvXGo"))
					.setHeader("Content-Type",constant("application/json"))
					.toD("https4://"+ansibleTowerUrl+"/api/v2/job_templates/${header.apbName}/launch/")
					.bean(RiskValidationBean.class,"readAnsibleResponse")
					.to("kafka:"+"ansiblestat"+ "?brokers=" + kafkaBootstrap);







		}catch (Exception e) {
			e.printStackTrace();
		}


	}

	public HttpComponent createCustomHttp4Component() {
		HttpComponent httpComponent = new HttpComponent();

		httpComponent.setX509HostnameVerifier(AllowAllHostnameVerifier.INSTANCE);


		TrustManagersParameters trustManagersParameters = new TrustManagersParameters();
		X509ExtendedTrustManager extendedTrustManager = new InsecureX509TrustManager();
		trustManagersParameters.setTrustManager(extendedTrustManager);

		SSLContextParameters sslContextParameters = new SSLContextParameters();
		sslContextParameters.setTrustManagers(trustManagersParameters);
		httpComponent.setSslContextParameters(sslContextParameters);

		return httpComponent;
	}

	private static class InsecureX509TrustManager extends X509ExtendedTrustManager {
		@Override
		public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {

		}

		@Override
		public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {

		}

		@Override
		public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {

		}

		@Override
		public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {

		}

		@Override
		public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

		}

		@Override
		public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[0];
		}
	}


}

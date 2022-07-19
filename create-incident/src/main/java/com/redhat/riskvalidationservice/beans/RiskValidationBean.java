package com.redhat.riskvalidationservice.beans;

import com.google.gson.Gson;
import com.redhat.riskvalidationservice.datamodels.ApbRuns;
import com.redhat.riskvalidationservice.datamodels.Example;
import com.redhat.riskvalidationservice.datamodels.SensuEvents;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.jboss.resteasy.client.jaxrs.BasicAuthentication;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.engines.URLConnectionEngine;
import org.json.JSONObject;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.dmn.api.core.*;
import org.kie.dmn.core.util.KieHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.client.WebTarget;
import java.util.*;
import java.util.logging.Logger;

@Service
public class RiskValidationBean {

	private static final Logger LOG = Logger.getLogger(RiskValidationBean.class.getName());

	private static final String KSESSION = "";

	private final KieContainer kieContainer;

	@Autowired
	public RiskValidationBean(KieContainer kieContainer) {
		LOG.info("Initializing a new session.");
		this.kieContainer = kieContainer;
	}

	public String prepareAnsibleRequest(Exchange exchange) {

		DMNRuntime dmnRuntime = RuleSessionFactory.createDMNRuntime();
		String status = exchange.getIn().getBody().toString();

		String namespace = "https://kiegroup.org/dmn/_BBA88A7C-FD49-48EE-B20A-ED1FEC1D6200";
		String modelName = "CreateIncidentCheck";
		DMNModel dmnModel = dmnRuntime.getModel(namespace, modelName);

		SensuEvents sensuEvents = new SensuEvents();

		System.out.println("sensuEvents" + status);

		String jsonRequest = "{\"incidentCreatedDate\":15953904000000, \"incidentCreated\":false, \"status\":" + status
				+ ",\"currentDate\":" + new Date().getTime() + "}";

		System.out.println("jsonRequest" + jsonRequest);

		DMNContext dmnContext = dmnRuntime.newContext();
		dmnContext.set("Playbook Event", jsonRequest);

		DMNResult dmnResult = dmnRuntime.evaluateAll(dmnModel, dmnContext);

		DMNDecisionResult createIncident = dmnResult.getDecisionResultByName("Create Incident");

		return createIncident.getResult().toString();
	}

	public String createIncident(Exchange exchange) {

		System.out.println("inside block");
		String status = exchange.getIn().getBody().toString();

		System.out.println("sensuEvents" + status);
		String serviceNowApiCall = null;

		if (status.compareToIgnoreCase("false") != 0) {

			String host = exchange.getIn().getHeader("kafka.KEY").toString();

			// SensuEvents sensuEvents = new Gson().fromJson(status,SensuEvents.class);

			serviceNowApiCall = "{ \"short_description\": \"Issue needs assistance - Event : " + host
					+ "- Automation Remediation Failed\","
					+ "\"urgency\": \"2\","
					+ "\"impact\": \"2\","
					+ "\"assignment_group\": \"IT Help Desk\"}";
			System.out.println(serviceNowApiCall);
		}
		return serviceNowApiCall;

	}

}

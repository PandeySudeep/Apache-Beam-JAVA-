package com.equifax.creditsafe.transform;

import java.io.IOException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CallConsumerSSAWrapperService extends DoFn<String, String> {

	private static final long serialVersionUID = 9158201207495701833L;

	private static final Logger log = LoggerFactory.getLogger(CallConsumerSSAWrapperService.class);
	
	ValueProvider<String> fileNameSSA;
	ValueProvider<String> filePathSSA;
	ValueProvider<String> fileNameSSAResponse;
	ValueProvider<String> fileNamePathSSAResponse;
	ValueProvider<String> pgpEncryption;
	ValueProvider<String> serviceURLSSA;

	// callConsumerSSAWrapperSerivce(fileNameSSA,
	// filePathSSA,fileNameSSAResponse,fileNamePathSSAResponse,pgpEncryption,serviceURLSSA)));
	public CallConsumerSSAWrapperService(ValueProvider<String> fileNameSSA, ValueProvider<String> filePathSSA,
			ValueProvider<String> fileNameSSAResponse, ValueProvider<String> fileNamePathSSAResponse,
			ValueProvider<String> pgpEncryption, ValueProvider<String> serviceURLSSA) {

		this.fileNameSSA = fileNameSSA;
		this.filePathSSA = filePathSSA;
		this.fileNameSSAResponse = fileNameSSAResponse;
		this.fileNamePathSSAResponse = fileNamePathSSAResponse;
		this.pgpEncryption = pgpEncryption;
		this.serviceURLSSA = serviceURLSSA;
	}

	@ProcessElement
	public void processElement(ProcessContext c) throws ClientProtocolException, IOException {

		// String uri =
		// "http://10.149.16.43:8080/ic-ssa-wrapper-service/batch/search/ssa";
		String uri = serviceURLSSA.get();

		String payload = "{" + "\"inputFileLoc\":\"" + filePathSSA.get() + "\"," + "\"inputFileName\":\""
				+ fileNameSSA.get() + "\"," + "\"outputFileLoc\":\""
				+ fileNamePathSSAResponse.get() + "\"," + "\"outputFileName\":\"" + fileNameSSAResponse.get() + "\","
				+ "\"ssaServiceType\":" + "\"consumer\"," + "\"encryptionRequired\":\"" + pgpEncryption.get() + "\""
				+ "}";

		log.info(payload);
		StringEntity input = new StringEntity(payload);
		input.setContentType("application/json");

		HttpPost post = new HttpPost(uri);
		post.setEntity(input);

		try (CloseableHttpClient httpClient = HttpClients.createDefault();
				CloseableHttpResponse response = httpClient.execute(post)) {
			log.info("StatusCode: " + response.getStatusLine().getStatusCode());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

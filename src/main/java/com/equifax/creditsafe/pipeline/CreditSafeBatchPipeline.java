package com.equifax.creditsafe.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
//import org.apache.beam.sdk.coders.NullableCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.creditsafe.options.CreditSafeOptions;
import com.equifax.creditsafe.sql.CreditSafeSchema;
import com.equifax.creditsafe.transform.CallConsumerSSAWrapperService;
import com.equifax.creditsafe.transform.CreditSafeFileLoadSSATransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;


public class CreditSafeBatchPipeline{
	
	
	public static void main(String[] args) {

		CreditSafeOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(CreditSafeOptions.class);

		executePipeline(options);
	}
	
	@SuppressWarnings("serial")
	private static PipelineResult executePipeline(CreditSafeOptions options) {
		
		final Logger log = LoggerFactory.getLogger(CreditSafeOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		final ValueProvider<String> fileNameSSA = options.getSSAFileName();
		final ValueProvider<String> filePathSSA = options.getSSAFilePath();	
		ValueProvider<String> fileNameSSAResponse = options.getSSADestFileName();
		ValueProvider<String> fileNamePathSSAResponse = options.getSSADestFilePath();
		ValueProvider<String> pgpEncryption=options.getEncryptionRequired();
		ValueProvider<String> serviceURLSSA=options.getServiceURL();
		
		
		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);
				
		
		log.info("Applying transform: Read from Incoming CreditSafe data File for SSA Keying");
		
		//pgpCryptionEnabled=false

		PCollectionTuple creditSafeSrcSSARowsTuple = pipeline.apply("Read from Incoming CreditSafe File for SSA Keying",
				PGPDecryptFileIO.read().from(options.getInputPath()).withCryptorInfoSupplier(cryptorInfoSupplier))
				.apply("Convert To Response Row for SSA",
						ParDo.of(new CreditSafeFileLoadSSATransform(validRowsTag, invalidRowsTag))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		
							
		// Map File Rows As Per Schema
		PCollection<Row> rowsCrdSafSSAValidRows = creditSafeSrcSSARowsTuple.get(validRowsTag.getId());
		rowsCrdSafSSAValidRows.setRowSchema(CreditSafeSchema.schemaCreditSafeSSALoad());
		
				
		
		PCollection<Void> writeToFileCollection = rowsCrdSafSSAValidRows.apply("SSA Extract",new PGPEncryptFileIO<Row>(cryptorInfoSupplier,fileNameSSA, filePathSSA,getSSAExtract(),"|", null, null, false));
		
		
		//Mock Collection Created
		PCollection<String> mockRecordForServiceCall = pipeline .apply("Mock record for ServiceCall", Create.of("mock")).apply(Wait.on(writeToFileCollection));
		PCollection<String> serviceCallResponse = mockRecordForServiceCall.apply("Service Call",ParDo.of(new CallConsumerSSAWrapperService(fileNameSSA, filePathSSA,fileNameSSAResponse,fileNamePathSSAResponse,pgpEncryption,serviceURLSSA)));
			

		log.info("Running the pipeline");
		return pipeline.run();			
		
	}
	
	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getSSAExtract() {
    	
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
		config.put("SAFE_NUMBER", (c) -> c.element().getString("SAFE_NUMBER"));
		config.put("COMPANY_NAME", (c) -> c.element().getString("COMPANY_NAME"));        		
		config.put("ADDRESS", (c) -> c.element().getString("ADDRESS"));

    return config;
	}
	
}	
	
	
	
	
	
	
	
	
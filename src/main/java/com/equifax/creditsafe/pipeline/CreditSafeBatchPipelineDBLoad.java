package com.equifax.creditsafe.pipeline;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.creditsafe.helper.CustomJdbcHelper;
import com.equifax.creditsafe.options.CreditSafeOptions;
import com.equifax.creditsafe.sql.CreditSafeSchema;
import com.equifax.creditsafe.sql.CustomSQL;

import com.equifax.creditsafe.transform.CreditSafeFileLoadTransform;
import com.equifax.creditsafe.transform.CreditSafeSSARespLoadTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

public class CreditSafeBatchPipelineDBLoad {
	
	public static void main(String[] args) {
	CreditSafeOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
			.as(CreditSafeOptions.class);

	executePipeline(options);
	
	}
	
	@SuppressWarnings("serial")
	private static PipelineResult executePipeline(CreditSafeOptions options) {
		
		final Logger log = LoggerFactory.getLogger(CreditSafeOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		BarricaderInfo barricaderInfo = BarricaderInfo.create().withBarricaderOptions(options);
		
		ValueProvider<String> batchId = options.getBatchId();
		ValueProvider<String> errorfileName = options.getErrorFileName();
		
		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);
				
		String schemaCreditSafe = options.getCreditSafeSchemaName().get();
		
		log.info("Applying transform: Read from Incoming CreditSafe data File");
		
		//pgpCryptionEnabled=false

		PCollectionTuple creditSafeSrcRowsTuple = pipeline.apply("Read from Incoming CreditSafe File",
				PGPDecryptFileIO.read().from(options.getInputPath()).withCryptorInfoSupplier(cryptorInfoSupplier))
				.apply("Convert To Response Row",
						ParDo.of(new CreditSafeFileLoadTransform(validRowsTag, invalidRowsTag,batchId,barricaderInfo))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		
							
		// Map File Rows As Per Schema
		PCollection<Row> rowsCrdSafValidRows = creditSafeSrcRowsTuple.get(validRowsTag.getId());
		rowsCrdSafValidRows.setRowSchema(CreditSafeSchema.schemaCreditSafeLoad());
	
		
		log.info("Applying transform: Read from SSA Response file");
		
		//pgpCryptionEnabled=false

		PCollectionTuple creditSafeSSARespRowsTuple = pipeline.apply("Read from SSA Response file",
				PGPDecryptFileIO.read().from(options.getInputSSARespPath()).withCryptorInfoSupplier(cryptorInfoSupplier))
				.apply("Convert To Response Row",
						ParDo.of(new CreditSafeSSARespLoadTransform(validRowsTag, invalidRowsTag,barricaderInfo))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		
							
		// Map File Rows As Per Schema
		PCollection<Row> rowsCrdSafSSARespValidRows = creditSafeSSARespRowsTuple.get(validRowsTag.getId());
		rowsCrdSafSSARespValidRows.setRowSchema(CreditSafeSchema.schemaCreditSafeSSARespLoad());
	
		
		log.info("Applying transform: Create the Inner Join between Src File and SSA Resp File ");			 
		PCollectionTuple tuplesJoin = PCollectionTuple.of("SRC", rowsCrdSafValidRows).and("SSA", rowsCrdSafSSARespValidRows);
		
		PCollection<Row> outputresultSet_InJ = tuplesJoin.apply("Create the Join between Src File & SSA Resp File ",SqlTransform
				.query(CustomSQL.joinStmt))	
				.apply("Convert to CreditSafe object", ParDo.of(new DoFn<Row, Row>() {
					@SuppressWarnings("static-access")
					@ProcessElement
					public void processElement(ProcessContext c) {
						Row row = (Row) c.element();
						Row.Builder outputRowBuilder = Row.withSchema(CreditSafeSchema.schemaCreditSafeJoinMap());
						
						row.getString("SAFE_NUMBER");
						row.getString("EFXID");
						row.getString("COMPANY_NAME");
						row.getString("ADDRESS");
						row.getString("CITY");
						row.getString("STATE");
						row.getString("ZIP");
						row.getString("_LIMIT");
						row.getString("RATING");
						row.getString("SCORECARD");
						row.getString("TRADE_LINES");
						row.getString("BALANCE_TOTAL");
						row.getString("DBT");
						//row.getString("CREATE_DT");
						row.getString("SAFE_NUMBER_HMAC");
						row.getString("COMPANY_NAME_HMAC");
						row.getString("ADDRESS_HMAC");
						row.getString("CITY_HMAC");
						row.getString("STATE_HMAC");
						row.getString("ZIP_HMAC");
						row.getString("STREET_ADDR_SSA_KEY");
						row.getString("CUST_NM_SSA_KEY");
						row.getString("BATCH_ID");
												
						c.output(row);
					}
				}));
		outputresultSet_InJ.setRowSchema(CreditSafeSchema.schemaCreditSafeJoinMap());
		
		 PCollection<Row> rejectedRows = new JdbcWriter<Row>("Write to output table",
					dbConnectionInfo, () -> {
						return JdbcCommons.applySchemaToQuery(CustomSQL.insertCreditSafeLoad,
								Arrays.asList(schemaCreditSafe));
					}, (row, preparedStatement) -> {
						CustomJdbcHelper.rowsCreditSafeLoad(row, preparedStatement, batchId);
					}, null, SerializableCoder.of(Row.class),
					JdbcWriter.newTupleTag("CaptureRejectedRecordsForCreditSafe"))
							.executeForOnlyRejectedOutput(outputresultSet_InJ)
							.setRowSchema(CreditSafeSchema.schemaCreditSafeJoinMap());
		 
		 
		 rejectedRows.apply("Error Entries",new PGPEncryptFileIO<Row>(cryptorInfoSupplier,errorfileName, options.getErrorOutputPath(),getDBWriteError(), "|", null, null, false));	
			
		
		log.info("Running the pipeline");
		return pipeline.run();	
		
	}
	
	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getDBWriteError() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
        		config.put("SAFE_NUMBER", (c) -> c.element().getString("SAFE_NUMBER"));
        		config.put("STREET_ADDR_SSA_KEY", (c) -> c.element().getString("STREET_ADDR_SSA_KEY"));

        return config;
    }
	
}

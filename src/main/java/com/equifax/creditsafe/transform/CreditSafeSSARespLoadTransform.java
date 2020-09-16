package com.equifax.creditsafe.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.sdk.schemas.Schema;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.values.Row;

import org.apache.beam.sdk.values.TupleTag;

import com.equifax.creditsafe.errors.InvalidFieldException;
import com.equifax.creditsafe.sql.CreditSafeSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.google.common.base.Splitter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CreditSafeSSARespLoadTransform extends DoFn<String, Row> {

	private static final long serialVersionUID = 9158201207495701833L;

	private static final Logger log = LoggerFactory.getLogger(CreditSafeSSARespLoadTransform.class);
	
	TupleTag validRowsTag;
	TupleTag invalidRowsTag;
	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;
	
	public CreditSafeSSARespLoadTransform(TupleTag validRowsTag, TupleTag invalidRowsTag,BarricaderInfo barricaderInfo) {

		this.validRowsTag = validRowsTag;
		this.invalidRowsTag = invalidRowsTag;
		this.barricaderInfo = barricaderInfo;
	}
	
	@StartBundle
	public void startBundle() throws Exception {

		barricader = Barricader.create().withInfo(barricaderInfo).andBuild();
	}
	
	@ProcessElement
	public void processElement(ProcessContext c) {

		try {
			
			// to ignore the header
			if (null != c.element() && !c.element().toUpperCase().contains("EFXID|COMPANY_NAME|AD")) {
			Row row = extractRowBuilder(CreditSafeSchema.schemaCreditSafeSSARespLoad(), c.element());

			validate(row);

			c.output(this.validRowsTag, row);
			}
		} catch (Exception ex) {

			log.error("Exception occurred for: " + c.element(), ex);

			c.output(this.invalidRowsTag, c.element() + "|" + ex.getMessage());
		}
	}
	
	private String checkForNull(String input) {

		if (null != input && input.trim().length() > 0) {
			return input.trim();
		}

		return null;
	}

	private Row extractRowBuilder(Schema schema, String input) throws IOException {

		Row.Builder rowBuilder = Row.withSchema(schema);

		Iterator<String> rowSplitter = Splitter.on("|").trimResults().split(input).iterator();
		
		
		String CSAFE_ORGANIZATION_SSA_FK_ID = checkForNull(rowSplitter.next());
		//String encryptedSSAFKID = barricader.encryptIgnoreEmpty(CSAFE_ORGANIZATION_SSA_FK_ID);
		String hashedSSAFKID = barricader.hashIgnoreEmpty(CSAFE_ORGANIZATION_SSA_FK_ID);
		rowBuilder.addValue(CSAFE_ORGANIZATION_SSA_FK_ID); //CSAFE_ORGANIZATION_SSA_FK_ID
		
		
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		//rowBuilder.addValue(checkForNull(rowSplitter.next()));
		
		rowBuilder.addValue(hashedSSAFKID);
		
		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

		// TODO: write any validations if required
	}

}
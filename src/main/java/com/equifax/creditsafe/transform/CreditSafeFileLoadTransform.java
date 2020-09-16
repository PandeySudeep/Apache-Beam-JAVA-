package com.equifax.creditsafe.transform;

import java.io.IOException;
import java.util.Iterator;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.creditsafe.errors.InvalidFieldException;
import com.equifax.creditsafe.helper.SubStringAndDateFunctions;
import com.equifax.creditsafe.sql.CreditSafeSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.google.common.base.Splitter;

//import com.equifax.usis.dibi.batch.gcp.dataflow.common.utils.SubStringAndDateFunctions;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CreditSafeFileLoadTransform extends DoFn<String, Row> {

	private static final long serialVersionUID = 9158201207495701833L;

	private static final Logger log = LoggerFactory.getLogger(CreditSafeFileLoadTransform.class);
	
	TupleTag validRowsTag;
	TupleTag invalidRowsTag;
	ValueProvider<String> batchId;
	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;
	
	public CreditSafeFileLoadTransform(TupleTag validRowsTag, TupleTag invalidRowsTag,ValueProvider<String> batchId,BarricaderInfo barricaderInfo) {

		this.validRowsTag = validRowsTag;
		this.invalidRowsTag = invalidRowsTag;
		this.batchId = batchId;
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
			Row row = extractRowBuilder(CreditSafeSchema.schemaCreditSafeLoad(), c.element());

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

		String safeNumber = checkForNull(rowSplitter.next());
		String encryptedSafeNo = barricader.encryptIgnoreEmpty(safeNumber);
		String hashedSafeNo = barricader.hashIgnoreEmpty(safeNumber);
		rowBuilder.addValue(encryptedSafeNo); //encrypted safe number
		
		
		
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//efx id
		
		String compName = checkForNull(rowSplitter.next());
		String encryptedCompName = barricader.encryptIgnoreEmpty(compName);
		String hashedCompName = barricader.hashIgnoreEmpty(compName);
		rowBuilder.addValue(encryptedCompName); //encrypted company name
		
		String address = checkForNull(rowSplitter.next());
		String encryptedAddress = barricader.encryptIgnoreEmpty(address);
		String hashedAddress = barricader.hashIgnoreEmpty(address);
		rowBuilder.addValue(encryptedAddress); //encrypted address
		
		String city = checkForNull(rowSplitter.next());
		String encryptedCity = barricader.encryptIgnoreEmpty(city);
		String hashedCity = barricader.hashIgnoreEmpty(city);
		rowBuilder.addValue(encryptedCity); //encrypted city
		
		String state = checkForNull(rowSplitter.next());
		String encryptedState = barricader.encryptIgnoreEmpty(state);
		String hashedState = barricader.hashIgnoreEmpty(state);
		rowBuilder.addValue(encryptedState); //encrypted state
		
		String zip = checkForNull(rowSplitter.next());
		String encryptedZip = barricader.encryptIgnoreEmpty(zip);
		String hashedZip = barricader.hashIgnoreEmpty(zip);
		rowBuilder.addValue(encryptedZip); //encrypted zip
		
		
		
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//_limit
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//rating
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//scorecard
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//trade_lines
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//balance_total
		rowBuilder.addValue(checkForNull(rowSplitter.next()));//dbt
		//rowBuilder.addValue(SubStringAndDateFunctions.getCurrentDate());//create_date
		rowBuilder.addValue(batchId.get());//batch_id
		
		rowBuilder.addValue(hashedSafeNo);// hmac safe number
		rowBuilder.addValue(hashedCompName);// hmac company name
		rowBuilder.addValue(hashedAddress);// hmac address
		rowBuilder.addValue(hashedCity);// hmac city
		rowBuilder.addValue(hashedState);// hmac state
		rowBuilder.addValue(hashedZip);// hmac zip
		
		
		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

		// TODO: write any validations if required
	}

}
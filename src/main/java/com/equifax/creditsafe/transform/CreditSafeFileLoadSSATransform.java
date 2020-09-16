package com.equifax.creditsafe.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import org.apache.beam.sdk.schemas.Schema;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import org.apache.beam.sdk.values.TupleTag;

import com.equifax.creditsafe.errors.InvalidFieldException;
import com.equifax.creditsafe.sql.CreditSafeSchema;

import com.google.common.base.Splitter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CreditSafeFileLoadSSATransform extends DoFn<String, Row> {

	private static final long serialVersionUID = 9158201207495701833L;

	private static final Logger log = LoggerFactory.getLogger(CreditSafeFileLoadSSATransform.class);
	
	TupleTag validRowsTag;
	TupleTag invalidRowsTag;
	
	public CreditSafeFileLoadSSATransform(TupleTag validRowsTag, TupleTag invalidRowsTag) {

		this.validRowsTag = validRowsTag;
		this.invalidRowsTag = invalidRowsTag;
		
	}
	
	
	@ProcessElement
	public void processElement(ProcessContext c) {

		try {
			
			// to ignore the header
			if (null != c.element() && !c.element().toUpperCase().contains("EFXID|COMPANY_NAME|AD")) {
			Row row = extractRowBuilder(CreditSafeSchema.schemaCreditSafeSSALoad(), c.element());

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

	private Row extractRowBuilder(Schema schema, String input) {

		Row.Builder rowBuilder = Row.withSchema(schema);

		Iterator<String> rowSplitter = Splitter.on("|").trimResults().split(input).iterator();

		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		rowBuilder.addValue(checkForNull(rowSplitter.next()));
		
		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

		// TODO: write any validations if required
	}

}
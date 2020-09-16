package com.equifax.creditsafe.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class CustomJdbcHelper {

	
	private static final Logger log = LoggerFactory.getLogger(CustomJdbcHelper.class);
	
	public static void rowsCreditSafeLoad(Row row, PreparedStatement preparedStatement, ValueProvider<String> batchId) throws SQLException {

		try {
			//final Timestamp CURRENT_TIMESTAMP = new Timestamp(System.currentTimeMillis());
			JdbcCommons.setFromString(row, "SAFE_NUMBER", preparedStatement, 1);
			JdbcCommons.setFromString(row, "EFXID", preparedStatement, 2);
			JdbcCommons.setFromString(row, "COMPANY_NAME", preparedStatement, 3);
			JdbcCommons.setFromString(row, "ADDRESS", preparedStatement, 4);
			JdbcCommons.setFromString(row, "CITY", preparedStatement, 5);
			JdbcCommons.setFromString(row, "STATE", preparedStatement, 6);
			JdbcCommons.setFromString(row, "ZIP", preparedStatement, 7);
			JdbcCommons.setFromString(row, "_LIMIT", preparedStatement, 8);
			JdbcCommons.setFromString(row, "RATING", preparedStatement, 9);
			JdbcCommons.setFromString(row, "SCORECARD", preparedStatement, 10);
			JdbcCommons.setFromString(row, "TRADE_LINES", preparedStatement, 11);
			JdbcCommons.setFromString(row, "BALANCE_TOTAL", preparedStatement, 12);
			JdbcCommons.setFromString(row, "DBT", preparedStatement, 13);
			JdbcCommons.setFromString(row, "SAFE_NUMBER_HMAC", preparedStatement, 14);
			JdbcCommons.setFromString(row, "COMPANY_NAME_HMAC", preparedStatement, 15);
			JdbcCommons.setFromString(row, "ADDRESS_HMAC", preparedStatement, 16);
			JdbcCommons.setFromString(row, "CITY_HMAC", preparedStatement, 17);
			JdbcCommons.setFromString(row, "STATE_HMAC", preparedStatement, 18);
			JdbcCommons.setFromString(row, "ZIP_HMAC", preparedStatement, 19);
			JdbcCommons.setFromString(row, "STREET_ADDR_SSA_KEY", preparedStatement, 20);
			JdbcCommons.setFromString(row, "CUST_NM_SSA_KEY", preparedStatement, 21);
			
						
			preparedStatement.setString(22, batchId.get());
			//preparedStatement.setTimestamp(6, CURRENT_TIMESTAMP);should it be there?
			
		} catch (SQLException ex) {

			log.error("SQLExecpetion occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}
	
}

package com.equifax.creditsafe.sql;


import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class CreditSafeSchema {
	
	public static Schema schemaCreditSafeLoad() {

		return Schema.builder()
				.addNullableField("SAFE_NUMBER", FieldType.STRING)
				.addNullableField("EFXID", FieldType.STRING)
				.addNullableField("COMPANY_NAME", FieldType.STRING)
				.addNullableField("ADDRESS", FieldType.STRING)
				.addNullableField("CITY", FieldType.STRING)
				.addNullableField("STATE", FieldType.STRING)
				.addNullableField("ZIP", FieldType.STRING)
				.addNullableField("_LIMIT", FieldType.STRING)
				.addNullableField("RATING", FieldType.STRING)
				.addNullableField("SCORECARD", FieldType.STRING)
				.addNullableField("TRADE_LINES", FieldType.STRING)
				.addNullableField("BALANCE_TOTAL", FieldType.STRING)
				.addNullableField("DBT", FieldType.STRING)
				//.addNullableField("CREATE_DT", FieldType.DATETIME)
				.addNullableField("BATCH_ID", FieldType.STRING)
				.addNullableField("SAFE_NUMBER_HMAC", FieldType.STRING)
				.addNullableField("COMPANY_NAME_HMAC", FieldType.STRING)
				.addNullableField("ADDRESS_HMAC", FieldType.STRING)
				.addNullableField("CITY_HMAC", FieldType.STRING)
				.addNullableField("STATE_HMAC", FieldType.STRING)
				.addNullableField("ZIP_HMAC", FieldType.STRING)
				.build();
	}

	public static Schema schemaCreditSafeSSARespLoad() {

		return Schema.builder()
				.addNullableField("SAFE_NUMBER", FieldType.STRING)
				.addNullableField("NAME", FieldType.STRING)
				.addNullableField("ADDRESS", FieldType.STRING)
				.addNullableField("FILLER1", FieldType.STRING)
				.addNullableField("STREET_ADDR_SSA_KEY", FieldType.STRING)
				.addNullableField("ADDRESSTYPE", FieldType.STRING)
				.addNullableField("FILLER2", FieldType.STRING)
				.addNullableField("CUST_NM_SSA_KEY", FieldType.STRING)
				.addNullableField("CUST_NM_SSA_KEY_TYPE", FieldType.STRING)
				//.addNullableField("FILLER3", FieldType.STRING)
				
				.addNullableField("CSAFE_ORGANIZATION_SSA_FK_ID_HMAC",FieldType.STRING)
				
				
				.build();
	}
	
	
	public static Schema schemaCreditSafeSSALoad() {

		return Schema.builder()
				.addNullableField("SAFE_NUMBER", FieldType.STRING)
				.addNullableField("EFXID", FieldType.STRING)
				.addNullableField("COMPANY_NAME", FieldType.STRING)
				.addNullableField("ADDRESS", FieldType.STRING)
				.build();
	}
	
	public static Schema schemaCreditSafeJoinMap() {

		return Schema.builder()
				.addNullableField("SAFE_NUMBER", FieldType.STRING)
				.addNullableField("EFXID", FieldType.STRING)
				.addNullableField("COMPANY_NAME", FieldType.STRING)
				.addNullableField("ADDRESS", FieldType.STRING)
				.addNullableField("CITY", FieldType.STRING)
				.addNullableField("STATE", FieldType.STRING)
				.addNullableField("ZIP", FieldType.STRING)
				.addNullableField("_LIMIT", FieldType.STRING)
				.addNullableField("RATING", FieldType.STRING)
				.addNullableField("SCORECARD", FieldType.STRING)
				.addNullableField("TRADE_LINES", FieldType.STRING)
				.addNullableField("BALANCE_TOTAL", FieldType.STRING)
				.addNullableField("DBT", FieldType.STRING)
				//.addNullableField("CREATE_DT", FieldType.DATETIME)
				.addNullableField("SAFE_NUMBER_HMAC", FieldType.STRING)
				.addNullableField("COMPANY_NAME_HMAC", FieldType.STRING)
				.addNullableField("ADDRESS_HMAC", FieldType.STRING)
				.addNullableField("CITY_HMAC", FieldType.STRING)
				.addNullableField("STATE_HMAC", FieldType.STRING)
				.addNullableField("ZIP_HMAC", FieldType.STRING)
				.addNullableField("STREET_ADDR_SSA_KEY", FieldType.STRING)
				.addNullableField("CUST_NM_SSA_KEY", FieldType.STRING)
				.addNullableField("BATCH_ID", FieldType.STRING)
	
				.build();
	}
	
	//public static String Select_CreditSafe_File= "SELECT SAFE_NUMBER,EFXID,COMPANY_NAME,ADDRESS,CITY,STATE,ZIP,LIMIT,RATING,SCORECARD,TRADE_LINES,BALANCE_TOTAL,DBT FROM PCOLLECTION";

	//public static String INSERT_T_CREDITSAFE_LOAD_TABLE = "INSERT INTO {{1}}.T_CREDITSAFE_LOAD (SAFE_NUMBER,EFXID,COMPANY_NAME,ADDRESS,CITY,STATE,ZIP,_LIMIT,RATING,SCORECARD,TRADE_LINES,BALANCE_TOTAL,DBT) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
}
package com.equifax.creditsafe.sql;

public class CustomSQL {

	//Join Statement
	/*	public static final String joinStmt = "SELECT SSA.ACCOUNT_NUMBER," + 
				" SSA.ADDRESSKEY," + 
				" SSA.NAMEKEY," + 
				" SSA.NAMEKEYTYPE," +
				" SRC.BATCH_ID" +
				" FROM" + 
				" SRC,SSA WHERE" +
				" SRC.SAFE_NUMBER = SSA.ACCOUNT_NUMBER";
		*/
		public static final String joinStmt = "SELECT SRC.SAFE_NUMBER," +
				" SRC.EFXID," +
				" SRC.COMPANY_NAME," +
				" SRC.ADDRESS," +
				" SRC.CITY," +
				" SRC.STATE," +
				" SRC.ZIP," +
				" SRC._LIMIT," +
				" SRC.RATING," +
				" SRC.SCORECARD," +
				" SRC.TRADE_LINES," +
				" SRC.BALANCE_TOTAL," +
				" SRC.DBT," +
				//" SRC.CREATE_DT," +
				" SRC.SAFE_NUMBER_HMAC," +
				" SRC.COMPANY_NAME_HMAC," +
				" SRC.ADDRESS_HMAC," +
				" SRC.CITY_HMAC," +
				" SRC.STATE_HMAC," +
				" SRC.ZIP_HMAC," +
				" SSA.STREET_ADDR_SSA_KEY," + 
				" SSA.CUST_NM_SSA_KEY," + 
				" SRC.BATCH_ID" +
				" FROM" + 
				" SRC,SSA WHERE" +
				" SRC.SAFE_NUMBER_HMAC = SSA.CSAFE_ORGANIZATION_SSA_FK_ID_HMAC";
		
		public static final String insertCreditSafeLoad = "INSERT INTO {{1}}.CREDITSAFE_ONLINE"
				+ "(SAFE_NUMBER,EFX_ID,COMPANY_NAME,ADDRESS,CITY,STATE,ZIP,_LIMIT,RATING,SCORECARD,TRADE_LINES,BALANCE_TOTAL,DBT,SAFE_NUMBER_HMAC,COMPANY_NAME_HMAC,ADDRESS_HMAC,CITY_HMAC,STATE_HMAC,ZIP_HMAC,STREET_ADDR_SSA_KEY,CUST_NM_SSA_KEY,BATCH_ID)" + 
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
}

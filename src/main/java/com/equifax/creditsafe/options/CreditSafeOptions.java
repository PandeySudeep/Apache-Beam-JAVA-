package com.equifax.creditsafe.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.BarricaderOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;



public interface CreditSafeOptions extends GcpOptions,BarricaderOptions, PipelineOptions, JdbcIOOptions, PGPCryptorOptions, JdbcExecutionOptions, HashiCorpVaultOptions{
	
	@Description("Encryption Required")
	ValueProvider<String> getEncryptionRequired();
	void setEncryptionRequired(ValueProvider<String> value);
	
	@Description("Batch Id")
    @Validation.Required
    ValueProvider<String> getBatchId();
    void setBatchId(ValueProvider<String> value);
	
	@Description("Input File Path")
	@Validation.Required
	ValueProvider<String> getInputPath();
    void setInputPath(ValueProvider<String> value);
	
    @Description("Input SSA Response Path")
	@Validation.Required
	ValueProvider<String> getInputSSARespPath();
    void setInputSSARespPath(ValueProvider<String> value);
	
	@Description("SSA File Path")
	@Validation.Required
	ValueProvider<String> getSSAFilePath();
    void setSSAFilePath(ValueProvider<String> value);
	
    @Description("SSA File Name")
	@Validation.Required
	ValueProvider<String> getSSAFileName();
    void setSSAFileName(ValueProvider<String> value);
	
    @Description("SSA File Dest Path")
	@Validation.Required
	ValueProvider<String> getSSADestFilePath();
    void setSSADestFilePath(ValueProvider<String> value);
	
    @Description("SSA File Dest Name")
	@Validation.Required
	ValueProvider<String> getSSADestFileName();
    void setSSADestFileName(ValueProvider<String> value);
    
    @Description("SSA ServiceURL")
    @Validation.Required
    ValueProvider<String> getServiceURL();
    void setServiceURL(ValueProvider<String> value);
    
    @Description("Schema for CreditSafe Target Table")
	@Validation.Required
	ValueProvider<String> getCreditSafeSchemaName();
    void setCreditSafeSchemaName(ValueProvider<String> value);
    
    @Description("Error File Name")
    @Validation.Required
    ValueProvider<String> getErrorFileName();
    void setErrorFileName(ValueProvider<String> value);
    
    @Description("Error Output File Path")
    @Validation.Required
    ValueProvider<String> getErrorOutputPath();
    void setErrorOutputPath(ValueProvider<String> value);
    
    
}
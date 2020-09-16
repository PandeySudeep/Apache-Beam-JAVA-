
package com.equifax.creditsafe.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface BarricaderOptions extends PipelineOptions {

	@Description("Encryption Enabled")
	@Default.Boolean(true)
	ValueProvider<Boolean> getEncryptionEnabled();

	void setEncryptionEnabled(ValueProvider<Boolean> value);

	@Description("KMS Key Reference")
	@Validation.Required
	ValueProvider<String> getKmsKeyReference();

	void setKmsKeyReference(ValueProvider<String> value);

	@Description("Wrapped DEK Bucket Name")
	@Validation.Required
	ValueProvider<String> getWrappedDekBucketName();

	void setWrappedDekBucketName(ValueProvider<String> value);

	@Description("Wrapped DEK File Path")
	@Validation.Required
	ValueProvider<String> getWrappedDekFilePath();

	void setWrappedDekFilePath(ValueProvider<String> value);

	@Description("Validate and Provision Wrapped DEK")
	@Default.Boolean(false)
	ValueProvider<Boolean> getValidateAndProvisionWrappedDek();

	void setValidateAndProvisionWrappedDek(ValueProvider<Boolean> value);

}


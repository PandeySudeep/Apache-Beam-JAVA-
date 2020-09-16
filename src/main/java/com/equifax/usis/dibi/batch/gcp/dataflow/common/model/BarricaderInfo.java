
package com.equifax.usis.dibi.batch.gcp.dataflow.common.model;

import java.io.Serializable;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.BarricaderOptions;

//import com.equifax.creditsafe.options.BarricaderOptions;

public class BarricaderInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	ValueProvider<Boolean> encryptionEnabled;
	ValueProvider<String> kmsKeyReference;
	ValueProvider<String> wrappedDekFilePath;
	ValueProvider<String> wrappedDekBucketName;
	ValueProvider<Boolean> validateAndProvisionWrappedDek;

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	private BarricaderInfo() {

		this.encryptionEnabled = StaticValueProvider.of(Boolean.FALSE);
		this.validateAndProvisionWrappedDek = StaticValueProvider.of(Boolean.FALSE);
	}

	public static BarricaderInfo create() {
		return new BarricaderInfo();
	}

	public BarricaderInfo withEncryption(ValueProvider<Boolean> input) {
		this.encryptionEnabled = input;
		return this;
	}

	public BarricaderInfo withKmsKeyReference(ValueProvider<String> input) {
		this.kmsKeyReference = input;
		return this;
	}

	public BarricaderInfo withWrappedDekFilePath(ValueProvider<String> input) {
		this.wrappedDekFilePath = input;
		return this;
	}

	public BarricaderInfo withWrappedDekBucketName(ValueProvider<String> input) {
		this.wrappedDekBucketName = input;
		return this;
	}

	public BarricaderInfo withValidateAndProvisionWrappedDek(ValueProvider<Boolean> input) {
		this.validateAndProvisionWrappedDek = input;
		return this;
	}

	public BarricaderInfo withBarricaderOptions(BarricaderOptions input) {

		this.withEncryption(input.getEncryptionEnabled());
		this.withKmsKeyReference(input.getKmsKeyReference());
		this.withWrappedDekFilePath(input.getWrappedDekFilePath());
		this.withWrappedDekBucketName(input.getWrappedDekBucketName());
		this.withValidateAndProvisionWrappedDek(input.getValidateAndProvisionWrappedDek());

		return this;
	}

	public ValueProvider<Boolean> getEncryptionEnabled() {
		return encryptionEnabled;
	}

	public ValueProvider<String> getKmsKeyReference() {
		return kmsKeyReference;
	}

	public ValueProvider<String> getWrappedDekFilePath() {
		return wrappedDekFilePath;
	}

	public ValueProvider<String> getWrappedDekBucketName() {
		return wrappedDekBucketName;
	}

	public ValueProvider<Boolean> getValidateAndProvisionWrappedDek() {
		return validateAndProvisionWrappedDek;
	}

}

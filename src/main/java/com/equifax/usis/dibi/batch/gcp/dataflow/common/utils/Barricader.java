
package com.equifax.usis.dibi.batch.gcp.dataflow.common.utils;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.ic.saas.cloud.barricade.BarricadeHmacUtil;
import com.equifax.ic.saas.cloud.barricade.BarricadeUtil;
import com.equifax.ic.saas.cloud.cloudstorage.CloudStorageClient;
import com.equifax.ic.saas.cloud.config.BarricadeConfig;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;

public class Barricader {

	private static final Logger log = LoggerFactory.getLogger(Barricader.class);

	private BarricaderInfo barricaderInfo;

	private BarricadeConfig barricadeConfig;
	private BarricadeUtil barricadeUtil;

	private CloudStorageClient storageClient;
	private static final String KEK_VERSION_METADATA = "x-goog-meta-kek-version";
	private String wrappedDekKey;

	private Barricader() {
		this.barricaderInfo = BarricaderInfo.create();
	}

	public static Barricader create() {
		return new Barricader();
	}

	public Barricader withInfo(BarricaderInfo input) {
		this.barricaderInfo = input;
		return this;
	}

	public Barricader andBuild() throws Exception {

		log.info("Barricader - Encryption Enabled ? " + this.barricaderInfo.getEncryptionEnabled().get());

		this.barricadeConfig = new BarricadeConfig();
		this.barricadeConfig.setEncryptionEnabled(this.barricaderInfo.getEncryptionEnabled().get());

		this.barricadeUtil = BarricadeUtil.getInstance(this.barricadeConfig);

		this.storageClient = new CloudStorageClient();

		if (this.barricaderInfo.getValidateAndProvisionWrappedDek().get()) {

			this.wrappedDekKey = validateAndProvisionWrappedDek();

		} else {

			this.wrappedDekKey = retrieveWrappedDek();
		}

		return this;
	}

	public String getWrappedDekKey() {
		return wrappedDekKey;
	}

	private String retrieveWrappedDek() throws IOException {

		String wrappedDekValue = null;

		if (this.barricaderInfo.getEncryptionEnabled().get()) {

			wrappedDekValue = this.storageClient.readFile(this.barricaderInfo.getWrappedDekBucketName().get(),
					this.barricaderInfo.getWrappedDekFilePath().get());
		}

		return wrappedDekValue;
	}

	public String validateAndProvisionWrappedDek() throws IOException {

		String wrappedDekValue = null;

		if (!this.barricaderInfo.getEncryptionEnabled().get()) {
			return wrappedDekValue;
		}

		wrappedDekValue = retrieveWrappedDek();

		HashMap<String, String> metadataMap = new HashMap<String, String>();

		if (StringUtils.isEmpty(wrappedDekValue)) {

			log.info("Wrapped DEK doesn't exist, hence generating new one");
			String newWrappedDek = barricadeUtil.generateWrappedDek(this.barricaderInfo.getKmsKeyReference().get());

			metadataMap.put(KEK_VERSION_METADATA,
					barricadeUtil.getKekVersion(this.barricaderInfo.getKmsKeyReference().get()));

			log.info("Uploading newly generated wrapped DEK to bucket");
			BarricadeHmacUtil.uploadWrappedDekToBucket(newWrappedDek,
					this.barricaderInfo.getWrappedDekBucketName().get(),
					this.barricaderInfo.getWrappedDekFilePath().get(), metadataMap);

			wrappedDekValue = newWrappedDek;
		}

		try {

			log.info("Checking if KEK rotation has occurred");
			if (BarricadeHmacUtil.doesKEKRotated(this.barricaderInfo.getKmsKeyReference().get(),
					this.barricaderInfo.getWrappedDekBucketName().get(),
					this.barricaderInfo.getWrappedDekFilePath().get(), true)) {

				log.info("KEK rotation has occurred; hence rewrapping the DEK");
				String newWrappedDek = barricadeUtil.rewrapDek(wrappedDekValue,
						this.barricaderInfo.getKmsKeyReference().get());

				metadataMap.put(KEK_VERSION_METADATA,
						barricadeUtil.getKekVersion(this.barricaderInfo.getKmsKeyReference().get()));

				log.info("Uploading newly generated wrapped DEK to bucket");
				BarricadeHmacUtil.uploadWrappedDekToBucket(newWrappedDek,
						this.barricaderInfo.getWrappedDekBucketName().get(),
						this.barricaderInfo.getWrappedDekFilePath().get(), metadataMap, true);

				wrappedDekValue = newWrappedDek;
			}

		} catch (IOException ex) {

			log.error("Exception while checking, re-wrapping or uploading DEK with new rotated KEK", ex);
		}

		return wrappedDekValue;
	}

	public String encrypt(String request) throws IOException {

		if (this.barricaderInfo.getEncryptionEnabled().get()) {
			return barricadeUtil.encrypt(request, this.barricaderInfo.getKmsKeyReference().get());
		} else {
			return request;
		}
	}

	public String decrypt(String request) throws IOException {

		if (this.barricaderInfo.getEncryptionEnabled().get()) {
			return barricadeUtil.decrypt(request);
		} else {
			return request;
		}
	}

	public String hash(String valueToDigest) {

		if (this.barricaderInfo.getEncryptionEnabled().get()) {
			return barricadeUtil.hash(valueToDigest, getWrappedDekKey());
		} else {
			return valueToDigest;
		}
	}
	public String encryptIgnoreEmpty(String request) throws IOException {
		return isEmpty(request) ? request : encrypt(request);
	}

	public String decryptIgnoreEmpty(String request) throws IOException {
		return isEmpty(request) ? request : decrypt(request);
	}

	public String hashIgnoreEmpty(String request) throws IOException {
		return isEmpty(request) ? request : hash(request);
	}
	private boolean isEmpty(final CharSequence cs) {
		return cs == null || cs.length() == 0;
	
	}

}

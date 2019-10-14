package org.sharpsw.spark.utils

import com.amazonaws.services.s3.model.Region

/*
 * TODO: Read configuration from a config file
 */
object StorageUtilFactory {

  def getStorageUtil(cloudStorageProvider: String): StorageUtil = {
    cloudStorageProvider.toLowerCase().trim() match {
      case "--aws-s3" =>
        new AWSS3Util(
          "http://127.0.0.1:9000",
          Region.FAKE.toString(),
          "7EFHH3SM4KDNCHM4FAKE",
          "zNI+QYy9MMMjMEQFoZ0c+qPI0+7VFT+9514Lfake")
      case "--azure" =>
        new AzureUtil("UseDevelopmentStorage=true")
      case "--gcloud" =>
        // Not using GCP utility because of guava conflict with google storage and hadoop binaries
        new GCPS3Util(
          "https://storage.googleapis.com",
          "auto",
          "./.gcp_creds/gcloudstoragetesting-6f85cdfc77ea.json",
          "gcloudstoragetesting")
//        new S3Util("https://storage.googleapis.com", "auto", "", "")
      case _ => throw new IllegalArgumentException(s"$cloudStorageProvider is not supported")
    }
  }
}

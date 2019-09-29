package org.sharpsw.spark.utils

import java.io.File.separator
import java.io.{File, FileInputStream, FileOutputStream}

import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.azure.storage.StorageCredentials
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.azure.storage.StorageUri
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlockBlob

//import org.apache.log4j.Logger

object AzureUtil {
  //  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  val storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=testing190926;AccountKey=7h8thOtZ+fObDX8T4L+yFpQ+rKFwX7OHZsQ+kP3BIhTe9DZAP+cV2EQEgwt644D95ke/+mvIReUYSBI3ndypoA==;EndpointSuffix=core.windows.net"

  def downloadObject(containerName: String, pathToBlob: String, local: String = "."): Unit = {
    val path = new File(local)
    val fileName = if(pathToBlob.contains("/")) {
      pathToBlob.split("/").last
    } else {
      pathToBlob
    }
    val outputFile = new File(path.getAbsolutePath + separator + fileName)
//    val outputFile = new File("./london-crime.zip")
    val storageCreds = StorageCredentials.tryParseCredentials(storageConnectionString)

    val storageAccount = CloudStorageAccount.parse(storageConnectionString)
    val client = storageAccount.createCloudBlobClient()
    val container = client.getContainerReference(containerName)
    val blob: CloudBlockBlob = container.getBlockBlobReference(pathToBlob)
    blob.download(new FileOutputStream(outputFile))

  }

  def uploadFiles(bucket: String, prefix: String, localBasePath: String, files: List[String]): Unit = {
    files.foreach(item => uploadSingleFile(bucket, prefix + item.substring(localBasePath.length).replaceAll("\\\\", "/"), item))
  }

  private def uploadSingleFile(bucket: String, key: String, uploadFileName: String): Unit = {
    try {
      //      logger.info(s"Uploading file $uploadFileName to S3")
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }
}

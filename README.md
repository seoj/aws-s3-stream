# AWS S3 Streaming

## S3PrefixTarInputStream

Allows reading of multiple S3 objects in a key prefix as if they were a single stream of tar file.

## S3MultipartUploadOutputStream

Allows writing to an object in S3 using multipart upload as if it was a single output stream.

## Example usage

Streaming multiple files from one S3 bucket into a single compressed tar.gz to another bucket.

```java
AmazonS3 s3 = new AmazonS3Client();
String targetBucketName = "my-target-bucket";
String targetKey = "archive.tar";
String sourceBucketName = "my-source-bucket";
String sourcePrefix = "some/key/prefix";
// no need to set the content-length of the object metadata
ObjectMetadata objectMetadata = new ObjectMetadata();
ListObjectsRequest listObjectsRequest = new ListObjectsRequest(sourceBucketName, sourcePrefix, null, null, null);

// Wrap the S3MultipartUploadOutputStream in Apache Commons Compress' GzipCompressorOutputStream
try (OutputStream os = new GzipCompressorOutputStream(new S3MultipartUploadOutputStream(s3, targetBucketName, targetKey, objectMetadata)))
{
    // The input stream from S3 is already a .tar
    try (InputStream is = new S3PrefixTarInputStream(s3, listObjectsRequest))
    {
        // Using Apache Commons IO to stream IS to an OS
        IOUtils.copy(is, os);
    }
}
```
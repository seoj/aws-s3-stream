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

## How it works

### S3PrefixTarInputStream

As soon as the first byte is attempted to be read from the input stream, a new thread is started (let's call this the ReaderThread). The ReaderThread does a combination of AWS S3's ListObjects and GetObject using the supplied information in the ListObjectsRequest. Each result of GetObject returns a stream for a single file in S3. The ReaderThread reads from this stream and pipes the output which is fed back to the caller of the original S3PrefixTarInputStream read. The S3PrefixTarInputStream blocks read until data is available or end of stream is reached. All underlying streams are closed and the ReaderThread joined with the caller thread when the S3PrefixTarInputStream is closed.

### S3MultipartUploadOutputStream

As soon as the first byte is attempted to be written into the S3MultipartUploadOutputStream, a new thread is started (call this the WriterThread). The WriterThread initiates a multipart upload, then proceeds to buffer the written bytes in memory. When the buffer is full, it does an UploadPart using the buffered bytes as the content of the upload. Once S3MultipartUploadOutputStream is closed, the remaining bytes in the buffer are uploaded as the final part of the upload, then the CompleteMultipartUpload is done. The WriterThread is joined with the caller thread when the S3MultipartUploadOutputStream is closed.
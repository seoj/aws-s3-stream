/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package my.seoj.s3.stream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class S3MultipartUploadOutputStream extends OutputStream
{
    private AmazonS3 s3;

    private String bucketName;

    private String key;

    private ObjectMetadata objectMetadata;

    private InitiateMultipartUploadResult initiateMultipartUploadResult;

    private byte[] buffer = new byte[5 * 1024 * 1024];

    private int bufferIndex = 0;

    private List<PartETag> partETags = new ArrayList<>();

    public S3MultipartUploadOutputStream(AmazonS3 s3, String bucketName, String key, ObjectMetadata objectMetadata)
    {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.key = key;
        this.objectMetadata = objectMetadata;
    }

    @Override
    public void write(int b) throws IOException
    {
        if (initiateMultipartUploadResult == null)
        {
            resetBuffer();
            initiateMultipartUploadResult = s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, objectMetadata));
        }

        if (bufferIndex == buffer.length)
        {
            uploadPart(buffer.length);
            resetBuffer();
        }

        buffer[bufferIndex++] = (byte) b;
    }

    @Override
    public void close() throws IOException
    {
        if (bufferIndex > 0)
        {
            uploadPart(bufferIndex);
        }

        s3.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, initiateMultipartUploadResult.getUploadId(), partETags));
    }

    private void resetBuffer()
    {
        bufferIndex = 0;
        Arrays.fill(buffer, (byte) -1);
    }

    private void uploadPart(int partSize)
    {
        int partNumber = partETags.size() + 1;
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.setKey(key);
        uploadPartRequest.setUploadId(initiateMultipartUploadResult.getUploadId());
        uploadPartRequest.setPartNumber(partNumber);
        uploadPartRequest.setPartSize(partSize);
        uploadPartRequest.setInputStream(new ByteArrayInputStream(buffer));
        UploadPartResult uploadPartResult = s3.uploadPart(uploadPartRequest);
        String eTag = uploadPartResult.getETag();
        PartETag partETag = new PartETag(partNumber, eTag);
        partETags.add(partETag);
    }
}

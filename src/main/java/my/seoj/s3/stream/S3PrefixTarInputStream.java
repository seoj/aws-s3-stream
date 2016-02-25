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

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;

public class S3PrefixTarInputStream extends InputStream
{
    private AmazonS3 s3;

    private ListObjectsRequest listObjectsRequest;

    private TarArchiveOutputStream tarArchiveOutputStream;

    private PipedInputStream pipedInputStream;

    private Thread thread;

    public S3PrefixTarInputStream(AmazonS3 s3, ListObjectsRequest listObjectsRequest)
    {
        this.s3 = s3;
        this.listObjectsRequest = listObjectsRequest;
    }

    @Override
    public int read() throws IOException
    {
        if (tarArchiveOutputStream == null)
        {
            pipedInputStream = new PipedInputStream();
            PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);
            tarArchiveOutputStream = new TarArchiveOutputStream(pipedOutputStream);

            thread = new Thread(() -> {
                try
                {
                    while (true)
                    {
                        ObjectListing objectListing = s3.listObjects(listObjectsRequest);
                        List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
                        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
                        {
                            writeTarArchiveEntry(getS3Object(s3ObjectSummary));
                        }

                        if (objectListing.isTruncated())
                        {
                            listObjectsRequest.setMarker(objectListing.getNextMarker());
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    try
                    {
                        tarArchiveOutputStream.close();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
            thread.start();
        }

        return pipedInputStream.read();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            thread.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private S3Object getS3Object(S3ObjectSummary s3ObjectSummary)
    {
        String bucketName = s3ObjectSummary.getBucketName();
        String key = s3ObjectSummary.getKey();
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        return s3.getObject(getObjectRequest);
    }

    private void writeTarArchiveEntry(S3Object s3Object) throws IOException
    {
        TarArchiveEntry tarArchiveEntry = getTarArchiveEntry(s3Object);
        tarArchiveOutputStream.putArchiveEntry(tarArchiveEntry);
        try (S3ObjectInputStream objectContent = s3Object.getObjectContent())
        {
            IOUtils.copy(objectContent, tarArchiveOutputStream);
        }
        tarArchiveOutputStream.closeArchiveEntry();
    }

    private TarArchiveEntry getTarArchiveEntry(S3Object s3Object)
    {
        String tarArchiveEntryName = getTarArchiveEntryName(s3Object.getKey());
        TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(tarArchiveEntryName);
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
        long contentLength = objectMetadata.getContentLength();
        tarArchiveEntry.setSize(contentLength);
        return tarArchiveEntry;
    }

    private String getTarArchiveEntryName(String s3ObjectKey)
    {
        String prefix = listObjectsRequest.getPrefix();
        int prefixLength = prefix.length();
        return s3ObjectKey.substring(prefixLength);
    }
}

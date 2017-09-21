/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.impl;

import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.*;
import static org.apache.hadoop.fs.s3a.Constants.*;

public class CarbonS3FileSystem extends S3AFileSystem {

    private final TransferManagerConfiguration transferConfig = new TransferManagerConfiguration();
    private URI uri;


    private File stagingDirectory;


    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        requireNonNull(uri, "uri is null");
        requireNonNull(conf, "conf is null");

        setConf(conf);

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
                new Path(PATH_SEPARATOR).makeQualified(this.uri, new Path(PATH_SEPARATOR));

        CarbonProperties defaults = CarbonProperties.getInstance();
        if(defaults.getProperty(S3_ACCESS_KEY) != null) {
            conf.set(ACCESS_KEY, defaults.getProperty(S3_ACCESS_KEY));
            conf.set(SECRET_KEY, defaults.getProperty(S3_SECRET_KEY));
        }
        this.stagingDirectory = new File(conf.get(S3_STAGING_DIRECTORY, "/tmp"));
        conf.set(MULTIPART_SIZE,"320000000");
        conf.set(MIN_MULTIPART_THRESHOLD,"320000000");
        conf.set(FAST_UPLOAD,"true");
        conf.set(PURGE_EXISTING_MULTIPART,"true");
        super.initialize(uri, conf);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        if (!stagingDirectory.exists()) {
            createDirectories(stagingDirectory.toPath());
        }
        if (!stagingDirectory.isDirectory()) {
            throw new IOException("Configured staging path is not a directory: " + stagingDirectory);
        }
        File tempFile = createTempFile(stagingDirectory.toPath(), "carbon-s3-", ".tmp").toFile();

        if (exists(f)) {
            InputStream stream = open(f).getWrappedStream();
            byte[] content = new byte[bufferSize];

            BufferedOutputStream outputStream =
                    new BufferedOutputStream(new FileOutputStream(tempFile));
            int totalSize = 0;
            int bytesRead;
            while ((bytesRead = stream.read(content)) != -1) {
                System.out.println(String.format("%d bytes read from stream", bytesRead));
                outputStream.write(content, 0, bytesRead);
                totalSize += bytesRead;
            }
            outputStream.close();

            FSDataOutputStream fStream=create(f, true, totalSize, Short.valueOf("0"), totalSize, null);

              return fStream;
        } else
            throw new IOException("file not found");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        // no need to do anything for S3
        return true;
    }

}

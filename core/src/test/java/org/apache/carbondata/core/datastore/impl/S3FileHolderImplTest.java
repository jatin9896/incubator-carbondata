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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.util.CarbonProperties;

import com.amazonaws.services.s3.AmazonS3Client;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class S3FileHolderImplTest {
  private static String filePath;
  private static S3FileHolderImpl s3FileHolder;
  private static CarbonProperties defaults;
  private static byte[] data;
  private static AmazonS3Client amazonS3Client;
  private static S3AInputStream s3AInputStream;

  @BeforeClass public static void setUp() {
    filePath = "/fakePath";
    s3FileHolder = new S3FileHolderImpl();
    data = "fake data".getBytes();
    amazonS3Client = new AmazonS3Client();
    new MockUp<CarbonS3FileSystem>() {
      @Mock public void initialize(URI uri, Configuration conf) throws IOException {

      }
    };
    s3AInputStream = new S3AInputStream("", "", 20L, amazonS3Client, null);

    new MockUp<CarbonS3FileSystem>() {
      @Mock public FSDataInputStream open(Path f) throws IOException {

        return new FSDataInputStream(new BufferedFSInputStream(s3AInputStream, 20));
      }
    };
  }

  @Test() public void testReadByteBuffer()
      throws IOException, NoSuchFieldException, IllegalAccessException {

    new MockUp<S3AInputStream>() {
      @Mock public int read(byte[] buffer, int offset, int length) throws IOException {
        return 20;
      }
    };
    new MockUp<FSDataInputStream>() {
      @Mock public void seek(long desired) throws IOException {

      }
    };

    ByteBuffer byteBuffer = s3FileHolder.readByteBuffer(filePath, 10L, 0);
    assertTrue(byteBuffer instanceof ByteBuffer);
    //        byteBuffer = s3FileHolder.readByteBuffer(filePath, 0);

  }

  @Test public void testReadByteArray()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    S3AInputStream carbonS3InputStream = new S3AInputStream("", "", 20L, amazonS3Client, null);

    new MockUp<S3AInputStream>() {
      @Mock public int read(byte[] buffer, int offset, int length) throws IOException {
        return 20;
      }
    };
    new MockUp<FSDataInputStream>() {
      @Mock public void seek(long desired) throws IOException {

      }
    };

    byte[] bytes = s3FileHolder.readByteArray("s3a://filePath", 10);

    assertTrue(bytes instanceof byte[]);

  }

}

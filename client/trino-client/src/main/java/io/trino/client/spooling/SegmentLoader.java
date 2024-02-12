/*
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
package io.trino.client.spooling;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

public class SegmentLoader
        implements AutoCloseable
{
    private static final Logger logger = Logger.getLogger(SegmentLoader.class.getPackage().getName());
    private final OkHttpClient client;

    public SegmentLoader()
    {
        this.client = new OkHttpClient();
    }

    public InputStream load(SpooledSegment segment)
            throws IOException
    {
        return loadFromURI(segment.getDataUri());
    }

    public InputStream loadFromURI(URI segmentUri)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(segmentUri.toString())
                .build();

        Response response = client.newCall(request).execute();
        ResponseBody body = response.body();

        if (response.isSuccessful()) {
            return delegatingInputStream(response, requireNonNull(body, "response body is null").source().inputStream(), segmentUri);
        }

        throw new IOException("Could not open segment for streaming " + response.code() + " " + response.message());
    }

    private void delete(URI segmentUri)
    {
        Request deleteRequest = new Request.Builder()
                .delete()
                .url(segmentUri.toString())
                .build();

        client.newCall(deleteRequest).enqueue(new Callback()
        {
            @Override
            public void onFailure(Call call, IOException cause)
            {
                logger.log(Level.WARNING, "Could not acknowledge spooled segment", cause);
            }

            @Override
            public void onResponse(Call call, Response response)
            {
            }
        });
    }

    private InputStream delegatingInputStream(Response response, InputStream delegate, URI segmentUri)
    {
        return new InputStream()
        {
            @Override
            public int read(byte[] b)
                    throws IOException
            {
                return delegate.read(b);
            }

            @Override
            public int read(byte[] b, int off, int len)
                    throws IOException
            {
                return delegate.read(b, off, len);
            }

            @Override
            public long skip(long n)
                    throws IOException
            {
                return delegate.skip(n);
            }

            @Override
            public int available()
                    throws IOException
            {
                return delegate.available();
            }

            @Override
            public void close()
                    throws IOException
            {
                response.close();
                delegate.close();
                delete(segmentUri);
            }

            @Override
            public void mark(int readlimit)
            {
                delegate.mark(readlimit);
            }

            @Override
            public void reset()
                    throws IOException
            {
                delegate.reset();
            }

            @Override
            public boolean markSupported()
            {
                return delegate.markSupported();
            }

            @Override
            public int read()
                    throws IOException
            {
                return delegate.read();
            }
        };
    }

    @Override
    public void close()
            throws Exception
    {
        client.dispatcher().executorService().shutdown();
    }
}

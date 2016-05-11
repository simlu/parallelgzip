/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertArrayEquals;

/**
 *
 * @author shevek
 */
public class ParallelGZIPInputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelGZIPInputStreamTest.class);

    private static class ByteArrayOutputBuffer extends ByteArrayOutputStream {

        @Nonnull
        public ByteArrayInputStream toInput() {
            return new ByteArrayInputStream(buf, 0, count);
        }
    }

    private void testPerformance(int len, boolean randomData) throws Exception {
        Random r = new Random();
        byte[] data = new byte[len];
        if (randomData) {
            r.nextBytes(data);
        }
        LOG.info("Data is " + data.length + " bytes.");

        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer();    // Reallocation will occur on the first iteration.

        for (int i = 0; i < 10; i++) {
            out.reset();
            long orig;
            {
                ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out);
                gzip.write(data);
                gzip.close();
                Stopwatch stopwatch = Stopwatch.createStarted();
                GZIPInputStream in = new GZIPInputStream(out.toInput(), 1024 * 1024);
                byte[] copy = ByteStreams.toByteArray(in);
                orig = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                assertArrayEquals(data, copy);
            }

            out.reset();

            {
                ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out);
                gzip.write(data);
                gzip.close();
                Stopwatch stopwatch = Stopwatch.createStarted();
                ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
                // todo: remove after block guessing is implemented
                in.setBlockSizes(gzip.blockSizes);
                byte[] copy = ByteStreams.toByteArray(in);
                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                double perc = orig * 100 / (double) elapsed;
                LOG.info("size=" + data.length + "; serial=" + orig + "; parallel=" + elapsed + "; perf=" + (int) perc + "%");
                assertArrayEquals(data, copy);
            }
        }
    }

    @Test
    public void testPerformance() throws Exception {
        testPerformance(0, false);
        testPerformance(1, false);
        testPerformance(4, false);
        testPerformance(16, false);
        testPerformance(64 * 1024 - 1, false);
        testPerformance(64 * 1024, false);
        testPerformance(64 * 1024 + 1, false);
        testPerformance(4096 * 1024 + 17, false);
        testPerformance(16384 * 1024 + 17, false);
        testPerformance(65536 * 1024 + 17, false);

        testPerformance(0, true);
        testPerformance(1, true);
        testPerformance(4, true);
        testPerformance(16, true);
        testPerformance(64 * 1024 - 1, true);
        testPerformance(64 * 1024, true);
        testPerformance(64 * 1024 + 1, true);
        testPerformance(4096 * 1024 + 17, true);
        testPerformance(16384 * 1024 + 17, true);
        testPerformance(65536 * 1024 + 17, true);
    }

    private void testThreads(int nthreads) throws Exception {
        Random r = new Random();
        byte[] data = new byte[16 * 1024 * 1024];
        r.nextBytes(data);
        LOG.info("Data is " + data.length + " bytes.");

        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer();    // Reallocation will occur on the first iteration.

        for (int i = 0; i < 10; i++) {
            out.reset();
            {
                ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out, nthreads);
                gzip.write(data);
                gzip.close();
                Stopwatch stopwatch = Stopwatch.createStarted();
                ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
                ByteStreams.toByteArray(in);
                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                LOG.info("nthreads=" + nthreads + "; parallel=" + elapsed);
            }
        }

        ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
        byte[] copy = ByteStreams.toByteArray(in);
        assertArrayEquals(data, copy);
    }

    @Test
    public void testThreads() throws Exception {
        LOG.info("AvailableProcessors = " + Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < 8; i++)
            testThreads(i);
    }
}

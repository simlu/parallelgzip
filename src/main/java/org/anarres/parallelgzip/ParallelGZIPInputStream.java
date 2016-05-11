/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.*;

/**
 *
 * @author shevek
 */
public class ParallelGZIPInputStream extends FilterInputStream {

    private final ExecutorService executor;
    private final int nthreads;
    private final ArrayBlockingQueue<Future<byte[]>> emitQueue;

    private final Inflater inflater;
    private final int bufferSize = 2 * 64 * 1024;
    private final byte[] buffer = new byte[bufferSize];
    private int totalBytesWritten = 0;

    // todo: remove after block guessing is implemented
    private ArrayList<Integer> blockSizes;

    public ParallelGZIPInputStream(@Nonnull InputStream in, @Nonnull ExecutorService executor, @Nonnegative int nthreads) throws IOException {
        super(in);
        readHeader(in);
        inflater = new Inflater(true);
        this.executor = executor;
        this.nthreads = nthreads;
        this.emitQueue = new ArrayBlockingQueue<Future<byte[]>>(nthreads);
    }

    public ParallelGZIPInputStream(@Nonnull InputStream in, @Nonnegative int nthreads) throws IOException {
        this(in, ParallelGZIPEnvironment.getSharedThreadPool(), nthreads);
    }

    public ParallelGZIPInputStream(InputStream in) throws IOException {
        this(in, Runtime.getRuntime().availableProcessors());
    }

    // todo: remove after block guessing is implemented
    public void setBlockSizes(ArrayList<Integer> blockSizes) {
        this.blockSizes = blockSizes;
    }

    // ------------------------

    /**
     * CRC-32 for uncompressed data.
     */
    protected CRC32 crc = new CRC32();

    /**
     * Indicates end of input stream.
     */
    protected boolean eos;

    private boolean closed = false;

    /**
     * Check to make sure that this stream has not been closed
     */
    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    /**
     * Reads uncompressed data into an array of bytes. If <code>len</code> is not
     * zero, the method will block until some input can be decompressed; otherwise,
     * no bytes are read and <code>0</code> is returned.
     * @param buf the buffer into which the data is read
     * @param off the start offset in the destination array <code>b</code>
     * @param len the maximum number of bytes read
     * @return  the actual number of bytes read, or -1 if the end of the
     *          compressed input stream is reached
     *
     * @exception  NullPointerException If <code>buf</code> is <code>null</code>.
     * @exception  IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>buf.length - off</code>
     * @exception ZipException if the compressed input data is corrupt.
     * @exception IOException if an I/O error has occurred.
     *
     */
    public int read(@Nonnull byte[] buf, int off, int len) throws IOException {
        ensureOpen();
        if (eos) {
            return -1;
        }

        int readLength = -1;
        try {
            readLength = inflater.inflate(buf, off, len);
            while (readLength == 0 && !eos) {
                if (blockSizes.isEmpty()) {
                    int compressedLen = super.read(this.buffer, off, 10);
                    System.arraycopy(this.buffer, compressedLen - 8, this.buffer, 0, 8);
                    checkTrailer();
                    eos = true;
                } else {
                    int compressedLen = super.read(this.buffer, off, blockSizes.remove(0));
                    totalBytesWritten += inflater.getBytesWritten();
                    inflater.reset();
                    inflater.setInput(this.buffer, 0, compressedLen);
                    readLength = inflater.inflate(buf, off, len);
                }
            }
            crc.update(buf, off, readLength);

        } catch (DataFormatException e) {
            e.printStackTrace();
        }
        return readLength;
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream.
     * @exception IOException if an I/O error has occurred
     */
    public void close() throws IOException {
        if (!closed) {
            super.close();
            eos = true;
            closed = true;
        }
    }

    /**
     * GZIP header magic number.
     */
    public final static int GZIP_MAGIC = 0x8b1f;

    /*
     * File header flags.
     */
    private final static int FTEXT      = 1;    // Extra text
    private final static int FHCRC      = 2;    // Header CRC
    private final static int FEXTRA     = 4;    // Extra field
    private final static int FNAME      = 8;    // File name
    private final static int FCOMMENT   = 16;   // File comment

    /*
     * Reads GZIP member header and returns the total byte number
     * of this member header.
     */
    private int readHeader(InputStream this_in) throws IOException {
        CheckedInputStream in = new CheckedInputStream(this_in, crc);
        crc.reset();
        // Check header magic
        if (readUShort(in) != GZIP_MAGIC) {
            throw new ZipException("Not in GZIP format");
        }
        // Check compression method
        if (readUByte(in) != 8) {
            throw new ZipException("Unsupported compression method");
        }
        // Read flags
        int flg = readUByte(in);
        // Skip MTIME, XFL, and OS fields
        skipBytes(in, 6);
        int n = 2 + 2 + 6;
        // Skip optional extra field
        if ((flg & FEXTRA) == FEXTRA) {
            int m = readUShort(in);
            skipBytes(in, m);
            n += m + 2;
        }
        // Skip optional file name
        if ((flg & FNAME) == FNAME) {
            do {
                n++;
            } while (readUByte(in) != 0);
        }
        // Skip optional file comment
        if ((flg & FCOMMENT) == FCOMMENT) {
            do {
                n++;
            } while (readUByte(in) != 0);
        }
        // Check optional header CRC
        if ((flg & FHCRC) == FHCRC) {
            int v = (int)crc.getValue() & 0xffff;
            if (readUShort(in) != v) {
                throw new ZipException("Corrupt GZIP header");
            }
            n += 2;
        }
        crc.reset();
        return n;
    }

    /*
     * Validate GZIP trailer.
     *
     * Currently concatenated gzip data sets are not supported.
     */
    private void checkTrailer() throws IOException {
        InputStream in = new ByteArrayInputStream(this.buffer);
        long crcValue = readUInt(in);
        if ((crcValue != crc.getValue())) {
            throw new ZipException("Corrupt GZIP trailer (crc)");
        }
        // rfc1952; ISIZE is the input size modulo 2^32
        totalBytesWritten += inflater.getBytesWritten();
        if ((readUInt(in) != (totalBytesWritten & 0xffffffffL))) {
            throw new ZipException("Corrupt GZIP trailer (bytes written)");
        }
    }

    /*
     * Reads unsigned integer in Intel byte order.
     */
    private long readUInt(InputStream in) throws IOException {
        long s = readUShort(in);
        return ((long)readUShort(in) << 16) | s;
    }

    /*
     * Reads unsigned short in Intel byte order.
     */
    private int readUShort(InputStream in) throws IOException {
        int b = readUByte(in);
        return (readUByte(in) << 8) | b;
    }

    /*
     * Reads unsigned byte.
     */
    private int readUByte(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new EOFException();
        }
        if (b < -1 || b > 255) {
            // Report on this.in, not argument in; see read{Header, Trailer}.
            throw new IOException(this.in.getClass().getName()
                    + ".read() returned value out of range -1..255: " + b);
        }
        return b;
    }

    private byte[] tmpbuf = new byte[128];

    /*
     * Skips bytes of input data blocking until all bytes are skipped.
     * Does not assume that the input stream is capable of seeking.
     */
    private void skipBytes(InputStream in, int n) throws IOException {
        while (n > 0) {
            int len = in.read(tmpbuf, 0, n < tmpbuf.length ? n : tmpbuf.length);
            if (len == -1) {
                throw new EOFException();
            }
            n -= len;
        }
    }
}

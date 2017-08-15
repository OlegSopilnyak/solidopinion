package oleg.sopilnyak.test1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.LongStream;

/**
 * Task to test sum from file
 */
public class Sum implements Runnable {
    private static final int CHUNK_SIZE = 1_000_000;
    private static final int BUFFER_SIZE = 8192;
    private volatile long total = 0;

    private String pathToFile = "target/classes/test1/simple.txt";
    private final Set<ScheduledFuture> activeThreads = new ConcurrentSkipListSet<>();

    public Sum() {
    }

    public Sum(String pathToFile) {
        this.pathToFile = pathToFile;
    }

    @Override
    public void run() {
        final File dataFile = new File(pathToFile);
        long size = dataFile.length();
        if (size < CHUNK_SIZE) {
            try {
                calculateSingle(dataFile);
                System.out.println("Sum is :" + total);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                calculateParallel(dataFile);
                System.out.println("Sum is :" + total);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void calculateParallel(File dataFile) throws IOException {
        long chunks = dataFile.length() / CHUNK_SIZE;
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
        executor.setMaximumPoolSize(10);

        // launch the threads
        LongStream.range(0, chunks).forEach(i -> {
            final ScheduledFuture future = executor.schedule(() -> {
                try {
                    calculateSingleChunk(dataFile, i * CHUNK_SIZE);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, 0, TimeUnit.MILLISECONDS);
            // collect the futures
            activeThreads.add(future);
        });
        // suspend call
        activeThreads.forEach(f -> {
            try {
                // waiting for Thread ends
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // calculate chunk of file
    private void calculateSingleChunk(File dataFile, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            FileChannel channel = raf.getChannel();
            long max_bytes = Math.min(channel.size() - offset, CHUNK_SIZE);
            calculateChunk(channel, offset, max_bytes);
        }
    }

    // calculate short file
    private void calculateSingle(File dataFile) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            FileChannel channel = raf.getChannel();
            long max_bytes = channel.size();
            calculateChunk(channel, 0, max_bytes);
        }
    }

    private void calculateChunk(FileChannel channel, long startPoint, long maxBytes) throws IOException {
        long red = startPoint;
        final int[] payloadInt = new int[BUFFER_SIZE / 4];
        do {
            final long read = Math.min(BUFFER_SIZE, maxBytes - red);
            final MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, red, read);
            final int payloadSize = Math.min(buf.remaining(), BUFFER_SIZE) / 4;

            buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(payloadInt, 0, payloadSize);
            int payload = Arrays.stream(payloadInt, 0, payloadSize - 1).sum();
            if (total + payload > Long.MAX_VALUE) {
                return;
            }
            total += payload;
            red += read;
        } while (red < maxBytes);
    }
}

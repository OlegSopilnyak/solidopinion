package oleg.sopilnyak.test1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Task to test sum from file
 */
public class Sum implements Runnable {
//    private static final int CHUNK_SIZE = 4;
        private static final int CHUNK_SIZE = 1_000_000;
    private static final int BUFFER_SIZE = 8192;
    private volatile long total = 0;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
    private String pathToFile = "target/classes/test1/simple.txt";

    public Sum() {
    }

    public Sum(String pathToFile) {
        this.pathToFile = pathToFile;
    }

    @Override
    public void run() {
        System.out.println("Sum task starts");
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
        executor.setMaximumPoolSize(10);
        final long chunks = dataFile.length() / CHUNK_SIZE;
        // launch the threads
        LongStream.range(0, chunks).mapToObj(i ->
            CompletableFuture.runAsync(() -> {
                        try {
                            calculateSingleChunk(dataFile, i * CHUNK_SIZE);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }, executor)
        ).collect(Collectors.toList())
                // waiting for all calculation done
                .forEach(f -> f.join());
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

    private void calculateChunk(FileChannel channel, long startPoint, long chunkSize) throws IOException {
        long offset = startPoint;
        long lastChannelPos = startPoint + chunkSize;
        final int[] payloadInt = new int[BUFFER_SIZE / 4];
        do {
            final long read = Math.min(BUFFER_SIZE, lastChannelPos - offset);
            final MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, offset, read);
            final int payloadSize = Math.min(buf.remaining(), BUFFER_SIZE) / 4;

            buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(payloadInt, 0, payloadSize);
            int payload = Arrays.stream(payloadInt, 0, payloadSize).sum();
            if (total + payload > Long.MAX_VALUE) {
                return;
            }
            total += payload;
            offset += read;
        } while (offset < lastChannelPos);
    }
}

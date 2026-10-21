package exchange.core2.revelator;

import org.agrona.PrintBufferUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;


public class Sha256Test {

    private static final String HMAC_SHA_256 = "HmacSHA256";

    private static final Logger log = LoggerFactory.getLogger(Sha256Test.class);


//    @Test
//    public void test() throws Exception {
//
//        HashSha256 hashSha256 = new HashSha256();
//
//        Random random = new Random(1L);
//
//        for (int i = 0; i < 1_000_000; i++) {
//
//            final int size = random.nextInt(100);
//            byte[] symbols = new byte[size];
//            random.nextBytes(symbols);
//
//            final byte[] hash = hashSha256.hash(symbols);
//            final byte[] hashReference = digest(symbols);
//
//            if (!Arrays.equals(hash, hashReference)) {
//                log.debug("RESULT:   {}", PrintBufferUtil.hexDump(hash));
//                log.debug("EXPECTED: {}", PrintBufferUtil.hexDump(hashReference));
//                throw new RuntimeException("Unexpected result");
//            }
//
//        }
//
//
//    }


    @Test
    public void testJdkGarbage() throws Exception {

        for (int threadsNum = 1; threadsNum <= 13; threadsNum++) {
            final ExecutorService executor = Executors.newCachedThreadPool();
            final List<CompletableFuture<Double>> completableFutures = IntStream.range(0, threadsNum)
                    .mapToObj(ignore -> CompletableFuture.supplyAsync(this::benchmarkJdk, executor))
                    .toList();

            final Optional<Double> sum = completableFutures.stream().map(CompletableFuture::join).reduce(Double::sum);

            log.debug("Sum: {} (threads={})", sum, threadsNum);

        }


    }

    private double benchmarkJdk() {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");

            Random random = new Random(3L);

            final int msgSize = 128;

            byte[] message = new byte[msgSize];
            byte[] hash = new byte[32];

            long ignore = 0L;

            random.nextBytes(message);

            for (int i = 0; i < 1_000_000; i++) {
                digest.update(message);
                digest.digest(hash, 0, 32);
                ignore += hash[8] + (i & 1);
            }

            log.debug("Warmup completed, running test...");

            final long timeStart = System.currentTimeMillis();

            final int iterations = 2_000_000;

            for (int i = 0; i < iterations; i++) {


//            digest.reset();
                digest.update(message);
                digest.digest(hash, 0, 32);

                ignore += hash[8] + (i & 1);

//            final byte[] hashReference = digest(message);
//
//            if (!Arrays.equals(hash, hashReference)) {
//                log.debug("RESULT:   {}", PrintBufferUtil.hexDump(hash));
//                log.debug("EXPECTED: {}", PrintBufferUtil.hexDump(hashReference));
//                throw new RuntimeException("Unexpected result");
//            }

            }

            final long totalMs = System.currentTimeMillis() - timeStart;
            final double megaHash = (0.001 * iterations) / totalMs;

            log.debug("Done ({}) {}MH", ignore & 1, String.format("%.3f", megaHash));

            return megaHash;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private byte[] digest(byte[] bytes) throws NoSuchAlgorithmException {


        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(bytes);
    }


    private void encryptHmac() throws NoSuchAlgorithmException, InvalidKeyException, ShortBufferException {

        final Random rand = new Random(-1L);

        byte[] secretKey = new byte[32];
        rand.nextBytes(secretKey);

        log.info("Secret: {}", PrintBufferUtil.hexDump(secretKey));

        final Mac mac = Mac.getInstance(HMAC_SHA_256);
        final SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey, HMAC_SHA_256);
        mac.init(secretKeySpec);

        final byte[] messageArray = new byte[64];


        final ByteBuffer messageByteBuf = ByteBuffer.wrap(messageArray);

        for (int i = 0; i < 3; i++) {

            log.info("");

            messageByteBuf.putLong(i);
            messageByteBuf.putLong(i + 1);
            messageByteBuf.putLong(i + 2);
            messageByteBuf.putLong(i + 3);

            log.info("messageArray1 {}", PrintBufferUtil.hexDump(messageArray));

            PrintBufferUtil.hexDump(messageArray);

            mac.reset();
            mac.update(messageArray, 0, 32);
            mac.doFinal(messageArray, 32);

            log.info("messageArray2 {}", PrintBufferUtil.hexDump(messageArray));

            messageByteBuf.flip();
        }

    }


}

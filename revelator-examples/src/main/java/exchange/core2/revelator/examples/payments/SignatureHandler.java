package exchange.core2.revelator.examples.payments;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class SignatureHandler {

    private final MessageDigest digest;

    private final static int MAX_MSG_SIZE = 256;

    final byte[] message = new byte[MAX_MSG_SIZE];
    final byte[] hash = new byte[32];

    final long[] localHashLong = new long[4];

    final ByteBuffer inputByteBuffer = ByteBuffer.wrap(message);
    final LongBuffer inputLongBuffer = inputByteBuffer.asLongBuffer();

    final ByteBuffer hashByteBuffer = ByteBuffer.wrap(hash);
    final LongBuffer hashLongBuffer = hashByteBuffer.asLongBuffer();


    public SignatureHandler() {
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Not thread safe !
     */
    public long[] signTransfer(final long sourceAccount,
                               final long destinationAccount,
                               final long amount,
                               final short currency,
                               final TransferType transferType,
                               final long secret) {

        final long[] result = new long[4];
        signTransfer(sourceAccount, destinationAccount, amount, currency, transferType, secret, result);
        return result;
    }

    /**
     * Not thread safe !
     */
    public boolean checkSignatureTransfer(final long sourceAccount,
                                          final long destinationAccount,
                                          final long amount,
                                          final short currency,
                                          final TransferType transferType,
                                          final long secret,
                                          final long[] buffer,
                                          final int offset) {

        signTransfer(sourceAccount, destinationAccount, amount, currency, transferType, secret, localHashLong);
        return Arrays.equals(localHashLong, 0, 4, buffer, offset, offset + 4);
    }


    /**
     * Not thread safe !
     */
    private void signTransfer(final long sourceAccount,
                              final long destinationAccount,
                              final long amount,
                              final short currency,
                              final TransferType transferType,
                              final long secret,
                              final long[] sha256buffer) {

        try {
            final int msgSize = 5;

            inputLongBuffer.put(sourceAccount);
            inputLongBuffer.put(destinationAccount);
            inputLongBuffer.put(amount);
            inputLongBuffer.put(((long) currency << 8) | transferType.getCode());
            inputLongBuffer.put(secret);
            inputLongBuffer.flip();

            digest.update(message, 0, msgSize);
            digest.digest(hash, 0, 32);


            hashLongBuffer.get(sha256buffer);
            hashLongBuffer.flip();

        } catch (DigestException ex) {
            throw new RuntimeException(ex);
        }
    }

}

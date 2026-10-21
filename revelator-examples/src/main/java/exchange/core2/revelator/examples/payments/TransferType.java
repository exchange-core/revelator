package exchange.core2.revelator.examples.payments;

public enum TransferType {

    DESTINATION_EXACT((byte) 38),
    SOURCE_EXACT((byte) 89);

    private final byte code;

    public byte getCode() {
        return code;
    }

    TransferType(byte code) {
        this.code = code;
    }

    public static TransferType fromByte(byte code) {
        switch (code) {
            case 38 -> {
                return DESTINATION_EXACT;
            }
            case 89 -> {
                return SOURCE_EXACT;
            }
            default -> throw new IllegalArgumentException("Unknown TransferType " + code);
        }
    }

}

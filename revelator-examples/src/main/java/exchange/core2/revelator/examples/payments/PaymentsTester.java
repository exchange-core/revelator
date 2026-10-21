package exchange.core2.revelator.examples.payments;

import exchange.core2.benchmarks.generator.clients.ClientsCurrencyAccountsGenerator;
import exchange.core2.benchmarks.generator.currencies.CurrenciesGenerator;
import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.utils.AffinityThreadFactory;
import exchange.core2.revelator.utils.LatencyTools;
import exchange.core2.collections.affinity.CpuAffinity;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.agrona.collections.Hashing;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class PaymentsTester {

    private static final Logger log = LoggerFactory.getLogger(PaymentsTester.class);

    private static final long SET_REFERENCE_TIME_CODE = 8121092016521817263L;
    private static final long END_BATCH_CODE = 7293651321864826354L;
    private static final long DUMP_STAT = 1073923874826736264L;

    private static final double FEE_K = 0.00375;

    public static void main(String[] args) throws InterruptedException {
        PaymentsTester paymentsTester = new PaymentsTester();
        paymentsTester.test();

    }

    public void test() throws InterruptedException {

        int seed = 1;

        final MutableLong controlCorrelationCounter = new MutableLong();

        final Random rand = new Random(seed);

        final Map<Integer, Double> currencies = CurrenciesGenerator.randomCurrencies(30, 1, seed);

        log.info("Currencies: {}", currencies);

        final int accountsToCreate = Integer.getInteger("payments.accounts", 10_000_000);
        final int transfersToCreate = 1_000_000;
        final int iterations = Integer.getInteger("payments.iterations", 10);

        final Map<Integer, Double> currencyRates = CurrenciesGenerator.generateRandomRates(currencies.keySet().stream(), 2.2, seed);
        log.info("Rates: {}", currencyRates);
        final Map<Integer, Map<Integer, Double>> currencyRatesMatrix = CurrenciesGenerator.createRatesMatrix(currencyRates);

        final CurrencyRateProcessor currencyRateProcessor = new CurrencyRateProcessor();
        currencyRatesMatrix.forEach((currencyFrom, map) ->
                map.forEach((currencyTo, rate) ->
                        currencyRateProcessor.updateRate((short) (int) currencyFrom, (short) (int) currencyTo, rate)));

        final Map<Short, PaymentsApi.FeeConfig> feeLimits = prepareFeeLimits(currencyRates);
        log.info("Fee Limits: {}", feeLimits);
//        feeLimits.forEach((k, v) -> log.debug("Fee Limit: {}:{}", k, v));


//        final int accountsToCreate = 40;
//        final int transfersToCreate = 40;

        log.info("Creating {} accounts ...", accountsToCreate);
//        final List<BitSet> clients = ClientsCurrencyAccountsGenerator.generateClients(accountsToCreate, currencies, seed);
        final long[] accounts = ClientsCurrencyAccountsGenerator.generateAccountsForTransfers(accountsToCreate, currencies, AccountsProcessor::mapToAccount, 40, seed);

        log.info("Generated {} accounts, creating secrets...", accounts.length);

        final LongLongHashMap secrets = new LongLongHashMap();
        Arrays.stream(accounts).forEach(acc -> secrets.put(acc, rand.nextLong()));

        log.info("Secrets created, creating payments core ...");
        final BlockingQueue<Long> syncQueue = new LinkedBlockingQueue<>(1);

        final ResponseHandler responseHandler = new ResponseHandler(syncQueue);

//        AffinityThreadFactory.ThreadAffinityMode affinityMode = AffinityThreadFactory.ThreadAffinityMode.AFFINITY_PHYSICAL_CORE;
        AffinityThreadFactory.ThreadAffinityMode affinityMode = AffinityThreadFactory.ThreadAffinityMode.AFFINITY_LOGICAL_CORE;
        final AffinityThreadFactory threadFactory = new AffinityThreadFactory(affinityMode);

//        final PaymentsCore paymentsCore = PaymentsCore.createSimple(responseHandler, threadFactory);

        // -Dpayments.mode=pipelined|parallel|simple, -Dpayments.threads=2^N (shards; +1 results thread +1 publisher)
        final String mode = System.getProperty("payments.mode", "pipelined");
        final int processingThreadsNum = Integer.getInteger("payments.threads", 4);
        final PaymentsCore paymentsCore = switch (mode) {
            case "parallel" -> PaymentsCore.createParallel(responseHandler, threadFactory, processingThreadsNum);
            case "simple" -> PaymentsCore.createSimple(responseHandler, threadFactory);
            default -> PaymentsCore.createPipelined(responseHandler, threadFactory, processingThreadsNum);
        };
        log.info("Configuration {} {} threads {}", mode, processingThreadsNum, affinityMode);

        log.info("Starting payments core ...");
        paymentsCore.start();

        final PaymentsApi paymentsApi = paymentsCore.getPaymentsApi();

        final MutableInt correlationId = new MutableInt();

        // set fees
        paymentsApi.adjustFee(
                System.nanoTime(),
                correlationId.getAndIncrement(),
                FEE_K,
                feeLimits);

        // set conversion rates
        log.info("Setting {}^2 cross conversion rates ...", currencyRatesMatrix.size() - 1);

        currencyRateProcessor.exportAllRates((currencyFrom, currencyTo, rate) ->
                paymentsApi.adjustCurrencyRate(
                        System.nanoTime(),
                        correlationId.getAndIncrement(),
                        (short) (int) currencyFrom,
                        (short) (int) currencyTo,
                        rate));

        log.info("Generating {}*{} transfers ...", iterations, transfersToCreate);
        final long generationStartMs = System.currentTimeMillis();
        final List<List<TransferTestOrder>> allTransfers = IntStream.range(0, iterations)
                .parallel()
                .mapToObj(i -> {
                    final int iterationSeed = Hashing.hash(seed + i);
                    final List<TransferTestOrder> transfers = generateTransfers(
                            transfersToCreate,
                            accounts,
                            feeLimits,
                            currencyRateProcessor,
                            secrets,
                            iterationSeed);
                    log.info("{}. Generated {} transfers (seed={})", i, transfers.size(), iterationSeed);
                    return transfers;
                })
                .collect(Collectors.toList());
        log.info("Generated all transfer in {}ms", System.currentTimeMillis() - generationStartMs);

        // TODO fix maxBalances
        log.info("Generating  maxBalances....");
        final LongLongHashMap maxBalances = createMaxBalances(allTransfers.stream().flatMap(Collection::stream));
        log.info("Generated {} maxBalances", maxBalances.size());
//        maxBalances.forEachKeyValue((acc, maxbal) -> log.debug("MAX-BAL: {}={}", acc, maxbal));

        try (CpuAffinity lock = CpuAffinity.acquireCore()) {

            log.info("Opening {} accounts with {} positive balances...", accounts.length, maxBalances.size());

            final long openAccountStartMs = System.currentTimeMillis();

            for (final long account : accounts) {

                paymentsApi.openAccount(System.nanoTime(), correlationId.getAndIncrement(), account, secrets.get(account));
                final long amount = maxBalances.get(account);

                if (amount != 0) {
                    final long amount1 = 9999999999999999L;
                    paymentsApi.adjustBalance(System.nanoTime(), correlationId.getAndIncrement(), account, amount1);
                }
            }

            flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, System.nanoTime(), 0L);

//        paymentsApi.customQuery(Revelator.MSG_TYPE_TEST_CONTROL, System.nanoTime(), controlCorrelationCounter.incrementAndGet(), 0L);
//        if (syncQueue.take() != controlCorrelationCounter.longValue()) {
//            throw new IllegalStateException();
//        }
            log.info("Accounts created in {}ms, starting benchmark...", System.currentTimeMillis() - openAccountStartMs);

            int transferSetIdx = 0;

            for (int tps1 = Integer.getInteger("payments.tpsFrom", 800_000); tps1 <= Integer.getInteger("payments.tpsTo", 7_000_000); tps1 += Integer.getInteger("payments.tpsStep", 100_000)) {

                final int tps = tps1 + (rand.nextInt(1000) - 500);

//            log.info("Adjusted {} accounts", maxBalances.size());

                if (transferSetIdx == allTransfers.size()) {
                    transferSetIdx = 0;
//                    log.info("Updating {} balances...", maxBalances.size());
//                    maxBalances.forEachKeyValue((account, amount) -> paymentsApi.adjustBalance(System.nanoTime(), correlationId.getAndIncrement(), account, amount));
//                    flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, System.nanoTime(), 0L);
                }

                final long picosPerCmd = (1024L * 1_000_000_000L) / tps;

                long plannedTimestampPs = 10_000_000L; // relative timestamp in 1/1024 ns units (~1 ps)
                long lastKnownTimestampPs = 0L;
                int nanoTimeRequestsCounter = 0;

                final long startTimeNs = System.nanoTime();
                // setting timer
                flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, startTimeNs, SET_REFERENCE_TIME_CODE);

                final List<TransferTestOrder> transfers = allTransfers.get(transferSetIdx);
                for (final TransferTestOrder order : transfers) {

                    plannedTimestampPs += picosPerCmd;

                    while (plannedTimestampPs > lastKnownTimestampPs) {

                        lastKnownTimestampPs = (System.nanoTime() - startTimeNs) << 10;

                        nanoTimeRequestsCounter++;

                        if (plannedTimestampPs > lastKnownTimestampPs) {

                            // spin until its time to send next command
                            Thread.onSpinWait();
//                        Thread.yield();
//                        LockSupport.parkNanos(1);
                        }
                    }

//            log.info("plannedTimestampPs={}", plannedTimestampPs);


                    // TODO send batches (to benchmark processing + handler part)
                    paymentsApi.transfer(
                            plannedTimestampPs,
                            correlationId.getAndIncrement(),
                            order.sourceAccount,
                            order.destinationAccount,
                            order.amount,
                            order.currency,
                            order.transferType,
                            order.sha256);
                }

                flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, startTimeNs, END_BATCH_CODE);

                final float processingTimeUs = (System.nanoTime() - startTimeNs) / 1000f;
                final float perfMt = (float) transfers.size() / processingTimeUs;
                final float targetMt = (float) tps / 1_000_000.0f;
                final String tag = String.format("%.2fns %.3f -> %.3f MT/s %.0f%%",
                        picosPerCmd / 1024.0, targetMt, perfMt, perfMt / targetMt * 100.0);


                final Histogram histogram = responseHandler.hdrRecorder.getIntervalHistogram();
                final Map<String, String> latencyReportFast = LatencyTools.createLatencyReportFast(histogram);
                log.info("{} {} nanotimes={} tsidx={}", tag, latencyReportFast, nanoTimeRequestsCounter, transferSetIdx);

                flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, startTimeNs, DUMP_STAT);

                transferSetIdx++;
            }
        }

        paymentsCore.stop();

        log.info("Done");

    }

    
    private Map<Short, PaymentsApi.FeeConfig> prepareFeeLimits(final Map<Integer, Double> currencyRates) {

        return currencyRates.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> (short) (int) e.getKey(),
                        e -> new PaymentsApi.FeeConfig(
                                1 + (long) (100.0 / e.getValue()),
                                10 + (long) (1000.0 / e.getValue()))));
    }

    private void flushAndWait(final MutableLong controlCorrelationCounter,
                              final BlockingQueue<Long> syncQueue,
                              final PaymentsApi paymentsApi,
                              final long correlationId,
                              final long data) throws InterruptedException {

        paymentsApi.customQuery(Revelator.MSG_TYPE_TEST_CONTROL, correlationId, controlCorrelationCounter.incrementAndGet(), data);

        if (syncQueue.take() != controlCorrelationCounter.longValue()) {
            throw new IllegalStateException();
        }
    }

    public static List<TransferTestOrder> generateTransfers(final int transfersNum,
                                                            final long[] accounts,
                                                            final Map<Short, PaymentsApi.FeeConfig> feeLimits,
                                                            final CurrencyRateProcessor currencyRateProcessor,
                                                            final LongLongHashMap secrets,
                                                            final int seed) {

        SignatureHandler signatureHandler = new SignatureHandler();

        final List<TransferTestOrder> transfersList = new ArrayList<>();

        final Random random = new Random(seed);

        for (int i = 0; i < transfersNum; i++) {
            final int idxFrom = random.nextInt(accounts.length);
            final int idxToRaw = random.nextInt(accounts.length - 1);
            final int idxTo = idxToRaw < idxFrom ? idxToRaw : idxToRaw + 1;

            final long srcAcc = accounts[idxFrom];
            final long dstAcc = accounts[idxTo];

            final short dstCurrency = AccountsProcessor.extractCurrency(dstAcc);

            // pick random currency (with required distribution)
            final int randomIndex = random.nextInt(accounts.length);
            final long randomCurrencyAccount = accounts[randomIndex];
            final short orderCurrency = AccountsProcessor.extractCurrency(randomCurrencyAccount);

            final TransferType transferType = random.nextBoolean()
                    ? TransferType.DESTINATION_EXACT
                    : TransferType.SOURCE_EXACT;

            final double xRate = currencyRateProcessor.getRate(dstCurrency, orderCurrency);

            final long minOrderAmount = switch (transferType) {

                case DESTINATION_EXACT -> (long) xRate;

                // because fee is subtracted from the amount, it is important to provide necessary
                // amount considering ORD->DST conversion
                case SOURCE_EXACT -> (long) (feeLimits.get(dstCurrency).maxFee() * xRate);
            };

            final long orderAmount = minOrderAmount + random.nextInt(100_000) + 1L;

            final long secret = secrets.get(srcAcc);
            final long[] signature = signatureHandler.signTransfer(srcAcc, dstAcc, orderAmount, orderCurrency, transferType, secret);

            transfersList.add(new TransferTestOrder(srcAcc, dstAcc, orderAmount, orderCurrency, transferType, signature));
        }

        return transfersList;
    }


    public static LongLongHashMap createMaxBalances(Stream<TransferTestOrder> transfers) {
        final LongLongHashMap balances = new LongLongHashMap();

        transfers.forEach(tx -> {

            // TODO if currency!=source - required conversion
            // TODO fees

            balances.addToValue(tx.sourceAccount, tx.amount);

        });

        return balances;
    }

    public static IntLongHashMap createTreasures(LongLongHashMap balances) {

        final IntLongHashMap treasureBalances = new IntLongHashMap();

        balances.forEachKeyValue(
                (account, amount) ->
                        balances.addToValue(AccountsProcessor.extractCurrency(account), amount));

        return treasureBalances;
    }


    private static class ResponseHandler implements IPaymentsResponseHandler {

        private final BlockingQueue<Long> syncQueue;
        private long startTimeNs = 0;

        private final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

        private long cx = 0;

        public ResponseHandler(BlockingQueue<Long> syncQueue) {
            this.syncQueue = syncQueue;
        }

        @Override
        public void commandResult(long timestamp,
                                  long correlationId,
                                  int resultCode,
                                  IRequestAccessor accessor) {

//            log.debug("commandResult: t={} {}->{}", timestamp, correlationId, resultCode);

            if (accessor instanceof ITestControlCmdAccessor) {

                final long operationCode = ((ITestControlCmdAccessor) accessor).getData(0);

                if (operationCode == END_BATCH_CODE) {
                    startTimeNs = 0;
                } else if (operationCode == SET_REFERENCE_TIME_CODE) {
//                log.info("received startTimeNs={}", timestamp);
                    hdrRecorder.reset();
                    startTimeNs = timestamp;
                }

                try {
                    syncQueue.put(correlationId);
                } catch (final InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

            } else {

//                if (cx++ == 100) {
//                    cx = 0;

                if (startTimeNs != 0) {
                    final long nanoTime = System.nanoTime();
                    final long latency = nanoTime - startTimeNs - (timestamp >> 10);

//                    log.info("timestamp={} latency={} nanoTime={} startTimeNs={}", timestamp, latency, nanoTime, startTimeNs);

                    hdrRecorder.recordValue(latency);
                }

//                }

            }
        }

        @Override
        public void balanceUpdateEvent(long account, long diff, long newBalance) {
            // log.debug("balanceUpdateEvent: {}->{}", account, newBalance);
        }
    }


    record TransferTestOrder(
            long sourceAccount,
            long destinationAccount,
            long amount,
            short currency,
            TransferType transferType,
            long[] sha256) {
    }
}

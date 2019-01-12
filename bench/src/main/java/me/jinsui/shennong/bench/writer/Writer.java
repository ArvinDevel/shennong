package me.jinsui.shennong.bench.writer;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

@Slf4j
public abstract class Writer implements Runnable {
    final AtomicBoolean isDone = new AtomicBoolean(false);
    final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );

    // stats
    final LongAdder eventsWritten = new LongAdder();
    final LongAdder bytesWritten = new LongAdder();

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running schema stream storage writer", e);
        }
    }

    abstract void execute() throws Exception;

    void markPerfDone() throws Exception {
        log.info("------------------- DONE -----------------------");
        printAggregatedStats(cumulativeRecorder);
        isDone.set(true);
        Thread.sleep(5000);
        System.exit(0);
    }

    void reportStats() {
        // Print report stats
        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            if (isDone.get()) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double rate = eventsWritten.sumThenReset() / elapsed;
            double throughput = bytesWritten.sumThenReset() / elapsed / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                "Throughput written : {}  records/s --- {} MB/s --- Latency: mean:"
                    + " {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                throughputFormat.format(rate), throughputFormat.format(throughput),
                dec.format(reportHistogram.getMean() / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                dec.format(reportHistogram.getMaxValue() / 1000.0));

            reportHistogram.reset();

            oldTime = now;
        }

    }


    private static final DecimalFormat throughputFormat = new DecimalFormat("0.0");
    private static final DecimalFormat dec = new DecimalFormat("0.000");

    static void printAggregatedStats(Recorder recorder) {
        Histogram reportHistogram = recorder.getIntervalHistogram();

        log.info("Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {}"
                + " - 99.9pct: {} - 99.99pct: {} - 99.999pct: {} - Max: {}",
            dec.format(reportHistogram.getMean() / 1000.0),
            dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
            dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
            dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
            dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
            dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
            dec.format(reportHistogram.getValueAtPercentile(99.999) / 1000.0),
            dec.format(reportHistogram.getMaxValue() / 1000.0));
    }

}

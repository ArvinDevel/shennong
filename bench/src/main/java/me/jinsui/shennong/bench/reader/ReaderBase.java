package me.jinsui.shennong.bench.reader;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

@Slf4j
public abstract class ReaderBase implements Runnable {
    final AtomicBoolean isDone = new AtomicBoolean(false);
    final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );

    // stats
    // for interval stats output
    final LongAdder eventsRead = new LongAdder();
    final LongAdder bytesRead = new LongAdder();
    // for final cumulative stats
    final LongAdder cumulativeEventsRead = new LongAdder();
    final LongAdder cumulativeBytesRead = new LongAdder();
    // used to calculate aggregated stats, needs set on subClass
    long startTime;

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running storage reader", e);
        }
    }

    abstract void execute() throws Exception;

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

            double rate = eventsRead.sumThenReset() / elapsed;
            double throughput = bytesRead.sumThenReset() / elapsed / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                "Throughput read : {}  records/s --- {} MB/s --- Latency: mean:"
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

    void printAggregatedStats(Recorder recorder) {
        long endTime = System.currentTimeMillis();
        double elapsed = (endTime - startTime) / 1e3;
        double rate = cumulativeEventsRead.sum() / elapsed;
        double throughput = cumulativeBytesRead.sum() / elapsed / 1024 / 1024;

        Histogram reportHistogram = recorder.getIntervalHistogram();

        log.info("Aggregated throughput read : {}  records/s --- {} MB/s, Aggregated E2E latency stats"
                + " --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {}"
                + " - 99.9pct: {} - 99.99pct: {} - 99.999pct: {} - Max: {}",
            throughputFormat.format(rate), throughputFormat.format(throughput),
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

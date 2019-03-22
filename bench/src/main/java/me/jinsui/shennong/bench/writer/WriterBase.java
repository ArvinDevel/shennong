package me.jinsui.shennong.bench.writer;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.BenchBase;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

@Slf4j
public abstract class WriterBase extends BenchBase {
    final AtomicBoolean isDone = new AtomicBoolean(false);
    final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );

    // stats
    // for interval stats output
    final LongAdder eventsWritten = new LongAdder();
    final LongAdder bytesWritten = new LongAdder();
    // for final cumulative stats
    final LongAdder cumulativeEventsWritten = new LongAdder();
    final LongAdder cumulativeBytesWritten = new LongAdder();
    long startTime;

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

    void printAggregatedStats(Recorder recorder) {
        long endTime = System.currentTimeMillis();
        double elapsed = (endTime - startTime) / 1e3;
        log.debug("before calculate, start time {}, end time {}, elapsed {}, total event is {}",
            startTime, endTime, elapsed, cumulativeEventsWritten.sum());
        log.debug("before calculate total bytes is {}", cumulativeBytesWritten.sum());
        double rate = cumulativeEventsWritten.sum() / elapsed;
        double throughput = cumulativeBytesWritten.sum() / elapsed / 1024 / 1024;

        Histogram reportHistogram = recorder.getIntervalHistogram();

        log.info("Aggregated throughput written : {}  records/s --- {} MB/s, Aggregated latency stats"
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

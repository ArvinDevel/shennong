package me.jinsui.shennong.bench;

import com.beust.jcommander.JCommander;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import me.jinsui.shennong.bench.writer.KafkaWriter;

@Slf4j
public class StreamStorageBench {

    private static JCommander commander;
    private static String usage = "Usage: ssbench write/read kafka/hdfs/cstream [args]";

    public static void main(String[] args) {
        parseArgsAndRun(args);
    }

    private static void parseArgsAndRun(String[] args) {
        if (args.length < 3) {
            log.info("Args is not efficient. \n {} \n current args are: {}", usage, args);
            return;
        }
        if (args[0].equals("write")) {
            if (args[1].equals("kafka")) {
                KafkaWriter.Flags kafkaFlags = new KafkaWriter.Flags();
                commander = JCommander.newBuilder()
                    .addObject(kafkaFlags)
                    .build();
                String[] subCmdArgs = Arrays.copyOfRange(
                    args, 2, args.length);
                try {
                    commander.parse(subCmdArgs);
                } catch (Exception e) {
                    log.warn("Parse exception ", e);
                    commander.usage();
                    return;
                }
                if (kafkaFlags.help) {
                    commander.usage();
                } else {
                    new KafkaWriter(kafkaFlags).run();
                }
            } else if (args[1].equals("hdfs")) {
                log.warn("Currently not implemented");
                return;
            }
        } else if (args[0].equals("read")) {
            log.warn("Currently not implemented");
            return;
        }
    }
}

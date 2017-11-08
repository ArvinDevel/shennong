package me.jinsui.shennong.test;

//import dlshade.org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
//import dlshade.org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;

/**
 *
 * Test LocalDLMEmulator & TestDistributedLogBase
 * ISSUE:
 * After Dlog bump bk version to 4.6, the LocalDLMEmulator in shaded/bkshade jar can't start bk,
 * we can set dlog version in the pom.xml
 */
public class LocalDLMEmulatorTest {

    static {
        // org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from 3.5.3 four letter words
        // are disabled by default due to security reasons
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }
    public static void main(String[] args) throws Exception {

        testLocalDLMEmulator();

    }

    /**
     *
     *   to test Dlog's localDLMEmulator when bk-common is imported as dependency.
     *   the default version 0.6.0-SnapShot is ok, because its dependency bk-server will load bk-common automatically.
     *  but the bk-shad jar is not ok, because it doesn't include bk-common,
     *  it's ok previously because MathUtil in bk is not refactored and the dlog use bk4.5
     */
    private static void testLocalDLMEmulator() throws Exception {
        LocalDLMEmulator localDLMEmulator = LocalDLMEmulator.newBuilder()
                .numBookies(1)
                .zkHost("127.0.0.1")
                .zkPort(2181)
                .shouldStartZK(true)
                .zkTimeoutSec(100)
                .build();
        localDLMEmulator.start();
        System.out.println("localDLMEmulator.start finish");
    }

}


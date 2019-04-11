package me.jinsui.shennong.bench.exp;

import com.scurrilous.circe.crc.Sse42Crc32C;
import me.jinsui.shennong.bench.source.CustomDataSource;

public class VerifySSE {

    public static void main(String[] args) {
        System.out.println("SSE supported " + Sse42Crc32C.isSupported());
        //test CustomDataSource
        new CustomDataSource(10,
            "/Users/arvin/IdeaProjects/shennong/bench/src/main/java/me/jinsui/shennong/bench/avro/customTest.avsc",
            10);
    }
}

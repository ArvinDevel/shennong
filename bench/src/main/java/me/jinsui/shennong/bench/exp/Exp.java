package me.jinsui.shennong.bench.exp;

import com.scurrilous.circe.crc.Sse42Crc32C;
import me.jinsui.shennong.bench.source.CustomDataSource;

public class Exp {

    public static void main(String[] args) {
//        customCg();
        cstreamBoundary();
    }

    static void verifySSE() {
        System.out.println("SSE supported " + Sse42Crc32C.isSupported());
        //test CustomDataSource
        new CustomDataSource(10,
            "/Users/arvin/IdeaProjects/shennong/bench/src/main/java/me/jinsui/shennong/bench/avro/customTest.avsc",
            10);
    }

    static void customCg() {
        try {
            new CustomCGCStreamWriter().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void cstreamBoundary() {
        try {
            new VerifyCStreamBoundary().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

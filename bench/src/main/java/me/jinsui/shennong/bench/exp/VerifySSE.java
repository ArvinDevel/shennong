package me.jinsui.shennong.bench.exp;

import com.scurrilous.circe.crc.Sse42Crc32C;

public class VerifySSE {

    public static void main(String[] args) {
        System.out.println("SSE supported " + Sse42Crc32C.isSupported());
    }
}

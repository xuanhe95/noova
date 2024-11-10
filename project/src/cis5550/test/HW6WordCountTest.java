package cis5550.test;

import cis5550.flame.FlameContextImpl;

public class HW6WordCountTest {
    public static void main(String[] args) {
        FlameContextImpl ctx = new FlameContextImpl("local");
        String text = "hello world hello hello world";
        String[] words = text.split(" ");
        try {
            FlameWordCount.run(ctx, words);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

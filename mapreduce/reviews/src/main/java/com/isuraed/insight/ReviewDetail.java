package com.isuraed.insight;

public class ReviewDetail implements Comparable<ReviewDetail> {
    private String productId;
    private String userId;
    private String text;
    private final long time;
    private final int score;

    public ReviewDetail(String productId, String userId, String text, long time, int score) {
        this.productId = productId;
        this.userId = userId;
        this.text = text;
        this.time = time;
        this.score = score;
    }

    public String getUserId() {
        return userId;
    }

    public long getTime() {
        return time;
    }

    public int getScore() {
        return score;
    }

    public int compareTo(ReviewDetail other) {
        return Long.compare(time, other.time);
    }

    public String toString() {
        return productId + "\t" + userId + "\t" + text + "\t" + time + "\t" + score;
    }
}

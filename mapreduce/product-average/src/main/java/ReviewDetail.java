public class ReviewDetail implements Comparable<ReviewDetail> {
    private String userId;
    private final long time;
    private final int score;

    public ReviewDetail(String userId, long time, int score) {
        this.userId = userId;
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
        return userId + "," + time + "," + score;
    }
}
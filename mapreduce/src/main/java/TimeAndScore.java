public class TimeAndScore implements Comparable<TimeAndScore> {
    private final long time;
    private final int score;

    public TimeAndScore(long time, int score) {
        this.time = time;
        this.score = score;
    }

    public long getTime() {
        return time;
    }

    public int getScore() {
        return score;
    }

    public int compareTo(TimeAndScore other) {
        return Long.compare(time, other.time);
    }

    public String toString() {
        return time + "," + score;
    }
}
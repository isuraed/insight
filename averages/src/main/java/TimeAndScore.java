public class TimeAndScore implements Comparable<TimeAndScore> {
    private final long time;
    private final long score;

    public TimeAndScore(long time, long score) {
        this.time = time;
        this.score = score;
    }

    public long getTime() {
        return time;
    }

    public long getScore() {
        return score;
    }

    public int compareTo(TimeAndScore other) {
        return Long.compare(time, other.time);
    }

    public String toString() {
        return time + "\t" + score;
    }
}
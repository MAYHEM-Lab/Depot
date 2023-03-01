package wtf.knc.depot.beam.streaming.objects;

/**
 * Java pojo class for message type Record
 */
public class Record {
    String name;
    String message;
    Long ets;

    public Record(String name, String message, Long ets) {
        this.name = name;
        this.message = message;
        this.ets = ets;
    }

    public Record() {
        super();
    }

    public static Record of(String name, String message, Long ets) {
        return new Record(name, message, ets);
    }

    public Long getTimestamp() {
        return ets;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getEts() {
        return ets;
    }

    public void setEts(Long ets) {
        this.ets = ets;
    }
}
package wtf.knc.depot.beam.streaming.coders;

import wtf.knc.depot.beam.streaming.objects.Record;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

public class RecordSerializableCoder extends Coder<Record> {
    private static final String NAMES_SEPARATOR = "_";

    @Override
    public void encode(Record record, OutputStream outStream) throws IOException {
        // Dummy encoder, separates first and last names by _
        String serializableRecord = record.getName() + NAMES_SEPARATOR + record.getMessage() + NAMES_SEPARATOR + record.getEts();
        outStream.write(serializableRecord.getBytes());
    }

    @Override
    public Record decode(InputStream inStream) throws CoderException, IOException {
        String serializedRecord = new BufferedReader(new InputStreamReader(inStream)).lines()
                .parallel().collect(Collectors.joining("\n"));
        String[] names = serializedRecord.split(NAMES_SEPARATOR);
        return Record.of(names[0], names[1], Long.parseLong(names[2]));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
}

package wtf.knc.depot.beam.streaming.loggers;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {

    private final String filenamePrefix;

    public WriteOneFilePerWindow(String filenamePrefix) {
        this.filenamePrefix = filenamePrefix;
    }

    @Override
    public PDone expand(PCollection<String> input) {
        // Verify that the input has a compatible window type.
        checkArgument(
                input.getWindowingStrategy().getWindowFn().windowCoder() == IntervalWindow.getCoder());

        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);

        return input.apply(
                TextIO.write()
                        .to(new PerWindowFiles(resource))
                        .withTempDirectory(resource.getCurrentDirectory())
                        .withWindowedWrites()
                        .withNumShards(1));
    }
}

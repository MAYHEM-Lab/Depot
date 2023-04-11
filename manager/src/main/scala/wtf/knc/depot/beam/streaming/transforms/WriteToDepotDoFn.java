package wtf.knc.depot.beam.streaming.transforms;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import wtf.knc.depot.beam.streaming.models.FileResponse;

import java.io.IOException;

public class WriteToDepotDoFn extends DoFn<KV<String, Long>, KV<String, Long>> {

    @ProcessElement
    public void processElement(ProcessContext c, OutputReceiver<KV<String, Long>> receiver) {
        try {
            System.out.println(new Gson().toJson(c.element()));
            FileResponse fileResponse = WriteToDepot.post("http://localhost:3000/api/entity/samridhim/files", "{\"parts\": \"1\", \"content_type\":\"text/plain\"}");
            String uploadId = fileResponse.getId();
            String name = fileResponse.getFilename();
            System.out.println("File Response:" + fileResponse.getParts().get(0));
            int uploadResponse = WriteToDepot.upload("http://localhost:3000"+fileResponse.getParts().get(0), new Gson().toJson(c.element()));
            System.out.println("Uploading File to S3 Response:" + uploadResponse);
            fileResponse = WriteToDepot.post("http://localhost:3000/api/entity/samridhim/files/"+fileResponse.getFilename(), "{\"upload_id\":\""+uploadId+"\"}");
            String datasetName = "streaming-data-wordcount";
            int responseCode = WriteToDepot.createDataset("http://localhost:3000/api/entity/samridhim/datasets/"+datasetName);
            System.out.println("response of create dataset: " + responseCode);
            fileResponse = WriteToDepot.post("http://localhost:3000/api/entity/samridhim/datasets/"+datasetName+"/upload", "{\"files\": {\"abc.txt\": \""+name+"\"}}");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(c.element());
        receiver.output(c.element());
    }
}

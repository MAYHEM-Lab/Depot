package wtf.knc.depot.beam.streaming.transforms;

import com.google.gson.Gson;
import okhttp3.*;
import okio.BufferedSink;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import wtf.knc.depot.beam.streaming.models.CreateDataset;
import wtf.knc.depot.beam.streaming.models.FileResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class WriteToDepot {
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    public static Gson gson = new Gson();

    public static FileResponse post(String url, String json) throws IOException {
        OkHttpClient client = new OkHttpClient();
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .header("Cookie", "access_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.dXNlcjoy.nEK30F5GxnLgkED3B7CFxvRIaJCdXDnctkANsji0DjA")
                .post(body)
                .build();
        System.out.println(request.toString());
        System.out.println(request.headers().toString());
        System.out.println(request.body());
        try (Response response = client.newCall(request).execute()) {
            System.out.println("Response Code:" + response.code());
            FileResponse fileResponse=gson.fromJson(response.body().string(), FileResponse.class);
            return fileResponse;
        }
    }

    public static int createDataset(String url) throws IOException {
        OkHttpClient client = new OkHttpClient();
        HashMap<String, String> datatype = new HashMap();
        datatype.put("type", "Raw");
        CreateDataset createDataset = new CreateDataset("streaming-dataset", "Unmanaged", datatype, "Public", "Weak", new ArrayList(), "true", new HashMap<String, String>());
        String requestBody = gson.toJson(createDataset, CreateDataset.class);
        System.out.println(requestBody);
        Request request = new Request.Builder()
                .url(url)
                .header("Cookie", "access_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.dXNlcjoy.nEK30F5GxnLgkED3B7CFxvRIaJCdXDnctkANsji0DjA")
                .post(RequestBody.create(JSON, requestBody))
                .build();
        System.out.println(request.toString());
        System.out.println(request.headers().toString());
        System.out.println(request.body());
        try (Response response = client.newCall(request).execute()) {
            System.out.println("Response Code:" + response.code());
            return response.code();
        }
    }


    public static int upload(String url, String content) throws IOException {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .header("Cookie", "access_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.dXNlcjoy.nEK30F5GxnLgkED3B7CFxvRIaJCdXDnctkANsji0DjA")
                .addHeader("Content-Type", "text/plain")
                .put(RequestBody.create(null, content))
                .build();
        System.out.println(request.toString());
        System.out.println(request.headers().toString());
        System.out.println(request.body().contentLength());
        try (Response response = client.newCall(request).execute()) {
            return response.code();
        }
    }
    
}

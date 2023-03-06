package wtf.knc.depot.beam.streaming.models;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class CreateDataset implements Serializable {

    @SerializedName("description")
    String description;
    String origin;
    HashMap<String, String> datatype;
    String visibility;

    @SerializedName("storage_class")
    String storageClass;
    ArrayList<String> triggers;
    String isolated;
    HashMap<String, String> content;

    public CreateDataset(String description, String origin, HashMap<String, String> datatype, String visibility, String storageClass, ArrayList<String> triggers, String isolated, HashMap<String, String> content) {
        this.description = description;
        this.origin = origin;
        this.datatype = datatype;
        this.visibility = visibility;
        this.storageClass = storageClass;
        this.triggers = triggers;
        this.isolated = isolated;
        this.content = content;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public HashMap<String, String> getDatatype() {
        return datatype;
    }

    public void setDatatype(HashMap<String, String> datatype) {
        this.datatype = datatype;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public ArrayList<String> getTriggers() {
        return triggers;
    }

    public void setTriggers(ArrayList<String> triggers) {
        this.triggers = triggers;
    }

    public String getIsolated() {
        return isolated;
    }

    public void setIsolated(String isolated) {
        this.isolated = isolated;
    }

    public HashMap<String, String> getContent() {
        return content;
    }

    public void setContent(HashMap<String, String> content) {
        this.content = content;
    }
}

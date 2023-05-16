package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BiolinkService {
    private static final String BIOLINK_URI = "https://raw.githubusercontent.com/biolink/biolink-model/master/biolink-model.yaml";
    public static JsonNode getBiolinkNode() {
        return getBiolinkNode(null);
    }

    @Cacheable("biolink")
    public static JsonNode getBiolinkNode(@Nullable String uri) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YAMLMapper yamlMapper = new YAMLMapper();
        JsonNode biolinkNode = mapper.nullNode();
        String specUri = BIOLINK_URI;
        if (uri != null) {
            specUri = uri;
        }
        try {
            biolinkNode = yamlMapper.readTree(new URL(specUri));
        } catch(MalformedURLException mex) {
            System.out.println("Bad URL");
            mex.printStackTrace();
        } catch (IOException iex) {
            System.out.println("IO issue");
            iex.printStackTrace();
        }
        return biolinkNode;
    }

    public static List<String> getClasses(JsonNode biolinkNode) {
        if (!biolinkNode.hasNonNull("classes")) {
            return Collections.emptyList();
        }
        List<String> skipClasses = new ArrayList<>();
        JsonNode classesNode = biolinkNode.get("classes");
        Iterator<String> fields = classesNode.fieldNames();
        String fieldName = "";
        while (fields.hasNext()) {
            fieldName = fields.next();
            JsonNode classNode = classesNode.get(fieldName);
            // There are no classes that have mixin, deprecated, or abstract values of "false"
            // so we treat the presence of the property as confirmation.
            if (classNode.hasNonNull("mixin") || classNode.hasNonNull("abstract") || classNode.hasNonNull("deprecated")) {
                skipClasses.add("biolink:" + fieldName.replace(" ", ""));
            }
        }
        return skipClasses;
    }
}

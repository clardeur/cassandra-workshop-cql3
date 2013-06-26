package fr.xebia.xke.cassandra;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;

public class JacksonReader {

    public static <T> T readJsonFile(Class<T> clazz, File file) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(file, clazz);
        } catch (Exception e) {
            System.out.println("Couldn't read file " + file.getName() + " : " + e.getMessage());
        }
        return null;
    }
}

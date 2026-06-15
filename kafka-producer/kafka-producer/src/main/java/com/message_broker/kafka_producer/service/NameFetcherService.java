package com.message_broker.kafka_producer.service;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import java.io.*;
import java.util.*;

@Service
@Slf4j
public class NameFetcherService {

    private final ResourceLoader resourceLoader;
    private final Random random = new Random();

    private List<String> cachedUserNames;
    private List<String> cachedCompanyNames;

    private boolean loadFailed = false;

    public NameFetcherService(final ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }


    @PostConstruct
    public void init() {
        try {
            cachedUserNames = loadNamesFromClasspath("classpath:names/RandomUserNames_EN.properties");
            log.info("Successfully loaded {} user names into cache", cachedUserNames.size());
        } catch (IOException e) {
            log.error("Failed to load user names file during initialization", e);
            cachedUserNames = Collections.emptyList();
        }

        try {
            cachedCompanyNames = loadNamesFromClasspath("classpath:names/RandomCompanyNames_EN.properties");
            log.info("Successfully loaded {} company names into cache", cachedCompanyNames.size());
        } catch (IOException e) {
            log.error("Failed to load company names file during initialization", e);
            cachedCompanyNames = Collections.emptyList();
        }
    }


    public String getRandomUserName() {
        if (loadFailed || cachedUserNames == null || cachedUserNames.isEmpty()) {
            log.warn("No names available in cache");
            return null;
        }
        return cachedUserNames.get(random.nextInt(cachedUserNames.size()));
    }

    public String getRandomCompanyName() {
        if (loadFailed || cachedCompanyNames == null || cachedCompanyNames.isEmpty()) {
            log.warn("No company names available in cache");
            return null;
        }
        return cachedCompanyNames.get(random.nextInt(cachedCompanyNames.size()));
    }



    private List<String> loadNamesFromClasspath(final String path) throws IOException {
        Resource resource = resourceLoader.getResource(path);
        if (!resource.exists()) {
            throw new FileNotFoundException(String.format("File not found: %s",path));
        }

        List<String> names = new ArrayList<>();


        try (InputStream is = resource.getInputStream()) {
            Properties props = new Properties();
            props.load(is);
            if (!props.isEmpty()) {
                for (Object value : props.values()) {
                    String name = value.toString().trim();
                    if (!name.isEmpty()) {
                        names.add(name);
                    }
                }
                if (!names.isEmpty()) {
                    return names;
                }
            }
        } catch (IOException e) {
            log.debug("Not a standard properties file, falling back to plain text", e);
        }


        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith("!")) {
                    names.add(line);
                }
            }
        }

        if (names.isEmpty()) {
            throw new IOException("No names found in the file");
        }
        return names;
    }
}
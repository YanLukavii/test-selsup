package ru;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.TimedSemaphore;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
@Data
public class CrptApi {

    private final Converter converter;

    private final TimedSemaphore timedSemaphore;

    private final HttpClient httpClient;

    private final Object lock = new Object();

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timedSemaphore = new TimedSemaphore(1, timeUnit, requestLimit);
        this.converter = new Converter();
        this.httpClient = HttpClient.newHttpClient();
    }

    public void createDocument(Document document, String signature) {
        synchronized (lock) {
            try {
                timedSemaphore.acquire();
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
            String json = createJsonFromDocument(document);
            HttpRequest request = createHttpRequest(json, signature);
            String responseBody = sendHttpRequest(request);
            log.info("Response: " + responseBody);
        }
    }

    private String createJsonFromDocument(Document document) {
        if (document == null) {
            log.error("Document is null and cannot be converted to JSON");
            return null;
        }
        return converter.convertToJson(document);
    }

    private HttpRequest createHttpRequest(String json, String signature) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
    }

    private String sendHttpRequest(HttpRequest request) {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    @Data
    public static class Converter {

        private final Gson gson = new Gson();

        public Document convertFileToDocument(String path) {

            try (FileReader reader = new FileReader(path)) {
                return gson.fromJson(reader, Document.class);

            } catch (IOException e) {
                log.error(e.getMessage());
            }
            return null;
        }

        public String convertToJson(Document doc) {
            return gson.toJson(doc);
        }
    }

    @Data
    public static class Description {

        @SerializedName("participantInn")
        private String participantInn;
    }

    @Data
    public static class Document {

        @SerializedName("description")
        private Description description;

        @SerializedName("doc_id")
        private String docId;

        @SerializedName("doc_status")
        private String docStatus;

        @SerializedName("doc_type")
        private String docType;

        @SerializedName("importRequest")
        private boolean importRequest;

        @SerializedName("owner_inn")
        private String ownerInn;

        @SerializedName("participant_inn")
        private String participantInn;

        @SerializedName("producer_inn")
        private String producerInn;

        @SerializedName("production_date")
        private String productionDate;

        @SerializedName("production_type")
        private String productionType;

        @SerializedName("products")
        private List<Product> products;

        @SerializedName("reg_date")
        private String regDate;

        @SerializedName("reg_number")
        private String regNumber;
    }

    @Data
    public static class Product {

        @SerializedName("certificate_document")
        private String certificateDocument;

        @SerializedName("certificate_document_date")
        private String certificateDocumentDate;

        @SerializedName("certificate_document_number")
        private String certificateDocumentNumber;

        @SerializedName("owner_inn")
        private String ownerInn;

        @SerializedName("producer_inn")
        private String producerInn;

        @SerializedName("production_date")
        private String productionDate;

        @SerializedName("tnved_code")
        private String tnvedCode;

        @SerializedName("uit_code")
        private String uitCode;

        @SerializedName("uitu_code")
        private String uituCode;
    }

    public static void runOneThreadCrptApiExample(String jsonDocPath, TimeUnit timeUnit, int requestLimit) {

        CrptApi crptApi = new CrptApi(timeUnit, requestLimit);
        Converter converter = crptApi.getConverter();
        Document document = converter.convertFileToDocument(jsonDocPath);

        IntStream.range(0, 20).forEach(i -> crptApi.createDocument(document, "Signature"));

        crptApi.getTimedSemaphore().shutdown();

    }

    public static void runManyThreadsCrptApiExample(String jsonDocPath, TimeUnit timeUnit,
                                                    int requestLimit, int countOfThreads) throws InterruptedException {

        CrptApi crptApi = new CrptApi(timeUnit, requestLimit);
        Converter converter = crptApi.getConverter();
        Document document = converter.convertFileToDocument(jsonDocPath);

        ExecutorService executor = Executors.newFixedThreadPool(countOfThreads);

        IntStream.range(0, countOfThreads).forEach(i -> {

            executor.execute(() -> {

                log.info("Thread " + Thread.currentThread().getId() + " started");

                IntStream.range(0, 20).forEach(j -> {
                    crptApi.createDocument(document, "Signature");
                });

                log.info("Thread " + Thread.currentThread().getId() + " finished");
            });
        });

        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        crptApi.getTimedSemaphore().shutdown();

    }

    public static void main(String[] args) throws InterruptedException {

        String filePath = "src/main/resources/json-doc.txt";

        CrptApi.runOneThreadCrptApiExample(filePath, TimeUnit.SECONDS, 5);
        //CrptApi.runManyThreadsCrptApiExample(filePath, TimeUnit.MINUTES, 5, 10);

    }
}
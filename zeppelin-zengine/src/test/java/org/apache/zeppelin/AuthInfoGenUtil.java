package org.apache.zeppelin;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NotebookImportDeserializer;
import org.apache.zeppelin.notebook.Paragraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthInfoGenUtil {

    private static AmazonS3 s3client;


    private static Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .setDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
            .registerTypeAdapter(Date.class, new NotebookImportDeserializer())
            .registerTypeAdapterFactory(Input.TypeAdapterFactory)
            .create();

    public static void main(String[] args) throws IOException {

        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        ClientConfiguration cliConf = createClientConfiguration();
        s3client = new AmazonS3Client(credentialsProvider, cliConf);
        s3client.setEndpoint("s3-us-west-2.amazonaws.com");
        System.setProperty("zeppelin.config.fs.dir","/xxx/zeppelin-zengine/src/test/resources/conf");

        loadAndRefreshAuthInfo();

    }


    public static void loadAndRefreshAuthInfo() throws IOException {
        NotebookAuthorizationRefresher notebookAuthorization = NotebookAuthorizationRefresher.getInstance();
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName("bucketName")
                    .withPrefix("rootFolder" + "/" + "notebook");
            ObjectListing objectListing;
            int i = 0;
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    if (objectSummary.getKey().endsWith("note.json")) {
                        Note note = getNote(objectSummary.getKey());
                        if (note != null) {
                            System.out.print("add note"+(++i)+ ":"+note.getId() + "==>");
                            List<Paragraph> paragraphs = note.getParagraphs();
                            if (CollectionUtils.isNotEmpty(paragraphs)){
                                if (StringUtils.isNotBlank(paragraphs.get(0).getUser())){

                                    Set<String> owners = notebookAuthorization.getOwners(note.getId());
                                    if (owners == null) {
                                        owners = new HashSet<>();
                                    }
                                    System.out.println(paragraphs.get(0).getUser());
                                    owners.add(paragraphs.get(0).getUser());
                                    if (owners.size() > 1){
                                        String owner_list = "";
                                        for (String s:owners){
                                            owner_list +=(s+",");
                                        }
                                        System.out.println("\t\t"+owner_list);
                                    }
                                    notebookAuthorization.setOwners(note.getId(),owners);
                                }else {
                                    System.out.println();
                                }
                            }
                        }
                        note = null;
                    }
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
        } catch (AmazonClientException ace) {
            throw new IOException("Unable to list objects in S3: " + ace, ace);
        }
        notebookAuthorization.saveToFile();
    }


    private static Note getNote(String key) throws IOException {
        S3Object s3object;
        try {
            s3object = s3client.getObject(new GetObjectRequest("cfdp", key));
        } catch (AmazonClientException ace) {
            throw new IOException("Unable to retrieve object from S3: " + ace, ace);
        }

        try (InputStream ins = s3object.getObjectContent()) {
            String json = IOUtils.toString(ins, "UTF-8");
            return gson.fromJson(json, Note.class);
        }
    }



    private static ClientConfiguration createClientConfiguration() {
        ClientConfigurationFactory configFactory = new ClientConfigurationFactory();
        ClientConfiguration config = configFactory.getConfig();


        return config;
    }
}
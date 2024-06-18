package io.backspace.util;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class S3Util {
    String bucketName = "lnp-mgmt-loa-entva0";
    Region region = Region.US_EAST_1;
    String folderPath = "lnpTester/portInOrders/pending";
    String destinationPath = "lnpTester/portInOrders/processed";
    //    String objectKey = "lnpTester/portInOrders/processed/testorder.csv";
    Instant thresholdTime = Instant.now().atZone(ZoneId.of("UTC"))
            .toInstant()
            .minusMillis(Duration.ofMinutes(30).toMillis());

    private S3Client pokeS3() {
        return S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    public void listObjects() {
        ListObjectsV2Request requestObject = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(folderPath)
                .build();
        ListObjectsV2Response response = pokeS3().listObjectsV2(requestObject);

        System.out.println(thresholdTime);


        for (S3Object summary : response.contents()) {
            if (summary.lastModified().isAfter(thresholdTime) &&
                    summary.key().endsWith(".csv")) {
                System.out.println("Last modified date: " + summary.lastModified());
                System.out.println("File name: "+summary.key());

                int fileNameIndex = summary.key().lastIndexOf("/") + 1;
                int fileExtensionIndex = summary.key().lastIndexOf(".");
                String destinationFileName = summary.key().substring(fileNameIndex, fileExtensionIndex)+"_"+System.currentTimeMillis()+".csv";

                CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                        .sourceBucket(bucketName)
                        .sourceKey(summary.key())
                        .destinationBucket(bucketName)
                        .destinationKey(destinationPath+"/"+destinationFileName)
                        .build();

                System.out.println("Destination: "+destinationPath+"/"+destinationFileName);
                pokeS3().copyObject(copyObjectRequest);

                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                                .bucket(bucketName)
                                        .key(summary.key())
                                                .build();
                pokeS3().deleteObject(deleteObjectRequest);
            }
        }
    }

    public void processFile() {
        /*try (S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
             ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Process each line of the CSV file
                    System.out.println(line);
                }
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        }*/
    }
}

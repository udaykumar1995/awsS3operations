package com.consistentlearners.s3search.S3SearchApplication;

import static com.amazonaws.util.IOUtils.copy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;

@SpringBootApplication
public class S3SearchApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(S3SearchApplication.class, args);
		final String BUCKET_NAME = "s3booksbucket";
		final String CSV_OBJECT_KEY = "book.csv";
		final String S3_SELECT_RESULTS_PATH = "D:\\Uday\\S3BucketFile\\book.csv";
		final String QUERY = "select * from S3Object s where s._1 = 'Kapil Isapuari'";

		AWSCredentials credentials = new BasicAWSCredentials(
				  "XXXXXXXXXXXXXXXXXXXXXXXX", 
				  "XXXXXXXXXXXXXX/XXXXXXXXXXXX/XXX"
				);
		AmazonS3 s3client = AmazonS3ClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_2)
				  .build();
		SelectObjectContentRequest request = generateBaseCSVRequest(BUCKET_NAME, CSV_OBJECT_KEY, QUERY);
		final AtomicBoolean isResultComplete = new AtomicBoolean(false);
		
		try (OutputStream fileOutputStream = new FileOutputStream(new File (S3_SELECT_RESULTS_PATH));
				SelectObjectContentResult result = s3client.selectObjectContent(request)) {
	            InputStream resultInputStream = result.getPayload().getRecordsInputStream(
	                    new SelectObjectContentEventVisitor() {
	                        @Override
	                        public void visit(SelectObjectContentEvent.StatsEvent event)
	                        {
	                            System.out.println(
	                                    "Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
	                                            +  " Bytes Processed: " + event.getDetails().getBytesProcessed());
	                        }

	                        @Override
	                        public void visit(SelectObjectContentEvent.EndEvent event)
	                        {
	                            isResultComplete.set(true);
	                            System.out.println("Received End Event. Result is complete.");
	                        }
	                    }
	            );

	            copy(resultInputStream, fileOutputStream);
	        }
		
		if (!isResultComplete.get()) {
			throw new Exception("S3 Select request was incomplete as End Event was not received.");
		}
	}

	private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
		SelectObjectContentRequest request = new SelectObjectContentRequest();
		request.setBucketName(bucket);
		request.setKey(key);
		request.setExpression(query);
		request.setExpressionType(ExpressionType.SQL);

		InputSerialization inputSerialization = new InputSerialization();
		inputSerialization.setCsv(new CSVInput());
		inputSerialization.setCompressionType(CompressionType.NONE);
		request.setInputSerialization(inputSerialization);

		OutputSerialization outputSerialization = new OutputSerialization();
		outputSerialization.setCsv(new CSVOutput());
		request.setOutputSerialization(outputSerialization);

		return request;
	}
}

// Imports the Google Cloud client library

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.datatransfer.v1.DataSource;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient.ListDataSourcesPagedResponse;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient.ListTransferConfigsPagedResponse;
import com.google.cloud.bigquery.datatransfer.v1.ListDataSourcesRequest;
import com.google.cloud.bigquery.datatransfer.v1.ListTransferConfigsRequest;
import com.google.cloud.bigquery.datatransfer.v1.ScheduleTransferRunsRequest;
import com.google.cloud.bigquery.datatransfer.v1.ScheduleTransferRunsResponse;
import com.google.cloud.bigquery.datatransfer.v1.TransferConfig;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.ScheduleTransferRunsRequest;
import com.google.cloud.bigquery.datatransfer.v1.ScheduleTransferRunsResponse;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import org.threeten.bp.Clock;
import org.threeten.bp.Instant;
import org.threeten.bp.temporal.ChronoUnit;

public class DataTransferApp {
  /**
   * List available data sources for the BigQuery Data Transfer service.
   */


  public static void main(String... args) throws Exception {
    // Sets your Google Cloud Platform project ID.
    String projectId = "bigqueryproject-303608";

    // Instantiate a client. If you don't specify credentials when constructing a client, the
    // client library will look for credentials in the environment, such as the
    // GOOGLE_APPLICATION_CREDENTIALS environment variable.
    try{
      DataTransferServiceClient client = DataTransferServiceClient.create();


      // Request the list of available data sources.
      String parent = String.format("projects/%s", projectId);

      ListDataSourcesRequest request =ListDataSourcesRequest.newBuilder().setParent(parent).build();

      ListDataSourcesPagedResponse response = client.listDataSources(request);

      // Print the results.
      System.out.println("Supported Data Sources:");
      for (DataSource dataSource : response.iterateAll()) {
        System.out.println(dataSource.getDisplayName());
        System.out.printf("\tID: %s%n", dataSource.getDataSourceId());
        System.out.printf("\tFull path: %s%n", dataSource.getName());
        System.out.printf("\tDescription: %s%n", dataSource.getDescription());
      }

      /** LIST OF SCHEDULED TRANSFER LIST */

      ListTransferConfigsRequest listTransferConfigsRequest = ListTransferConfigsRequest.newBuilder().setParent(parent).build();

      ListTransferConfigsPagedResponse listTransferConfigsPagedResponse = client.listTransferConfigs(listTransferConfigsRequest);

      TransferConfig tc = null;
      // Print the results.
      System.out.println("\n\nlist of TransferConfigs:");
      for (TransferConfig transferConfig : listTransferConfigsPagedResponse.iterateAll()) {
        System.out.println(transferConfig.getDisplayName());
        System.out.printf("\tID: %s%n", transferConfig.getDataSourceId());
        System.out.printf("\tFull path: %s%n", transferConfig.getName());
        System.out.printf("\tDescription: %s%n", transferConfig.getDisplayName());
        System.out.printf("\tSchedule: %s%n", transferConfig.getSchedule());
        System.out.printf("\tSchedule: %s%n", transferConfig.getAllFields());

        tc = transferConfig;
      }


      /** RUN THE SCHEDULED TRANSFER */

      String configId = tc.getName();
      Clock clock = Clock.systemDefaultZone();
      Instant instant = clock.instant();
      // (00:00AM UTC)
      Timestamp startTime =Timestamp.newBuilder()
              .setSeconds(instant.minus(5, ChronoUnit.DAYS).getEpochSecond())
              .setNanos(instant.minus(5, ChronoUnit.DAYS).getNano())
              .build();
      Timestamp endTime =Timestamp.newBuilder()
              .setSeconds(instant.minus(2, ChronoUnit.DAYS).getEpochSecond())
              .setNanos(instant.minus(2, ChronoUnit.DAYS).getNano())
              .build();
      
      scheduleBackFill(client,configId, startTime, endTime);

    }catch(Exception e){
        e.printStackTrace();
    }
  }


  public static void scheduleBackFill(DataTransferServiceClient client,String configId, Timestamp startTime, Timestamp endTime) throws IOException {
    try{
      ScheduleTransferRunsRequest request =ScheduleTransferRunsRequest.newBuilder().setParent(configId).setStartTime(startTime).setEndTime(endTime).build();

      ScheduleTransferRunsResponse response = client.scheduleTransferRuns(request);

      System.out.println("Schedule backfill run successfully :" + response.getRunsCount());
    } catch (ApiException ex) {
      System.out.print("Schedule backfill was not run." + ex.toString());
    }
  }


} 
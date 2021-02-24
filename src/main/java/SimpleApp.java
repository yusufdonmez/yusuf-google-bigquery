
import java.util.Date;
import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;


public class SimpleApp {

  /**  */
  public static void main(String... args) throws Exception {

    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    String sqlString ="insert into `bigqueryproject-303608.TestDataset.TestTable` (id,dd,ft,tt,agent,status) VALUES ('OID21401','2021-11-22','10:00:00','13:00:00',50000,'ready')";

    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlString).setUseLegacySql(false).build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    TableResult result = queryJob.getQueryResults();
    System.out.println("result "+result);

    for (FieldValueList row : result.iterateAll()) {
      System.out.printf("row %s", row.toString());
    }


    // testQuery(bigquery);
  }

  private static void testQuery(BigQuery bigquery) throws Exception {
    Long start = new Date().getTime();

    Long lap1 = new Date().getTime();
    System.out.println("lap1 "+(lap1-start));
    
    String sqlString  = "SELECT commit, author, repo_name FROM `bigquery-public-data.github_repos.commits` WHERE subject like '%bigquery%' ORDER BY subject DESC LIMIT 10";

    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlString).setUseLegacySql(false).build();

    Long lap2 = new Date().getTime();
    System.out.println("lap2 "+(lap2-lap1));
    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());


    Long lap3 = new Date().getTime();
    System.out.println("lap3 "+(lap3-lap2));
    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    Long lap4 = new Date().getTime();
    System.out.println("lap4 "+(lap4-lap3));
    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    TableResult result = queryJob.getQueryResults();
    System.out.println("result "+result);

    sqlString = "SELECT commit, author, repo_name FROM `bigquery-public-data.github_repos.commits` WHERE subject like '%bigquery%' ORDER BY subject DESC LIMIT 10";
    QueryJobConfiguration queryConfig2 = QueryJobConfiguration.newBuilder(sqlString).setUseLegacySql(false).build();

    
    JobId jobId2 = JobId.of(UUID.randomUUID().toString());
    Job queryJob2 = bigquery.create(JobInfo.newBuilder(queryConfig2).setJobId(jobId2).build());


    Long lap5 = new Date().getTime();
    System.out.println("lap5 "+(lap5-lap4));
    // Wait for the query to complete.
    queryJob2 = queryJob2.waitFor();

    Long lap6 = new Date().getTime();
    System.out.println("lap6 "+(lap6-lap5));
    // Check for errors
    if (queryJob2 == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob2.getStatus().getError() != null) {
      // You can also look at queryJob2.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob2.getStatus().getError().toString());
    }

    Long lap7 = new Date().getTime();
    System.out.println("lap7 "+(lap7-lap6));

    // Get the results.
    result = queryJob2.getQueryResults();

    Long lap8 = new Date().getTime();
    System.out.println("lap8 "+(lap8-start));
    // Print all pages of the results.
    for (FieldValueList row : result.iterateAll()) {
      // String type
      String commit = row.get("commit").getStringValue();
      // Record type
      FieldValueList author = row.get("author").getRecordValue();
      String name = author.get("name").getStringValue();
      String email = author.get("email").getStringValue();
      // String Repeated type
      String repoName = row.get("repo_name").getRecordValue().get(0).getStringValue();
      System.out.printf("Repo name: %s Author name: %s email: %s commit: %s\n", repoName, name, email, commit);
    }
    
    System.out.println("total "+(lap7-start));
  }
}
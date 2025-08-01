import * as cdk from 'aws-cdk-lib';
import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';

export class AwsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // ✅ 1. S3 Buckets
    const stageBucket = new s3.Bucket(this, 'RetailStageBucket', {
      bucketName: 'airflow-retail-stage',
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const martBucket = new s3.Bucket(this, 'RetailMartBucket', {
      bucketName: 'airflow-retail-mart',
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // ✅ 2. Glue Database
    const glueDb = new glue.CfnDatabase(this, 'RetailGlueDB', {
      catalogId: this.account,
      databaseInput: { name: 'retail_analytics_db' },
    });

    // ✅ 3. IAM Role for Glue Crawler
    const glueRole = new iam.Role(this, 'GlueCrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    // Grant glueRole read and list access on the mart bucket (crawler needs ListBucket + GetObject)
    martBucket.grantRead(glueRole);

    // ✅ 4. Glue Crawler
    new glue.CfnCrawler(this, 'RetailDataCrawler', {
      name: 'retail-parquet-crawler',
      role: glueRole.roleArn,
      databaseName: glueDb.ref,
      targets: {
        s3Targets: [
          {
            path: `s3://${martBucket.bucketName}/aggregates/`,
          },
        ],
      },
      tablePrefix: 'agg_',
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
      recrawlPolicy: {
        recrawlBehavior: 'CRAWL_EVERYTHING',
      },
    });
  }
}

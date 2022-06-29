import fs from 'fs';
import {
  DatasourceMetadataDto,
  DEFAULT_S3_PRESIGNED_URL_EXPIRATION_SECONDS,
  ExecutionOutput,
  IntegrationError,
  isReadableFile,
  isReadableFileConstructor,
  RawRequest,
  S3_ACTION_DISPLAY_NAMES,
  S3ActionConfiguration,
  S3ActionType,
  S3DatasourceConfiguration
} from '@superblocksteam/shared';
import { BasePlugin, PluginExecutionProps } from '@superblocksteam/shared-backend';
import { RequestFile } from '@superblocksteam/shared-backend';
import { S3, STS } from 'aws-sdk';
import { DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, PutObjectRequest } from 'aws-sdk/clients/s3';

export default class S3Plugin extends BasePlugin {
  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration,
    files
  }: PluginExecutionProps<S3DatasourceConfiguration>): Promise<ExecutionOutput> {
    try {
      const s3Client = this.getS3Client(datasourceConfiguration);
      const s3Action = actionConfiguration.action;
      const configuration = actionConfiguration;
      const ret = new ExecutionOutput();
      // TODO: Clean this up with a switch statement.
      if (s3Action === S3ActionType.LIST_OBJECTS) {
        if (!configuration.resource) {
          throw new IntegrationError('Resource required for list objects');
        }
        const data = await this.listObjects(s3Client, {
          Bucket: configuration.resource
        });
        ret.output = data.Contents;
      } else if (s3Action === S3ActionType.LIST_BUCKETS) {
        const data = await this.listBuckets(s3Client);
        ret.output = data.Buckets;
      } else if (s3Action === S3ActionType.GET_OBJECT) {
        if (!configuration.resource) {
          throw new IntegrationError('Resource required for get objects');
        }
        if (!configuration.path) {
          throw new IntegrationError('Path required for get objects');
        }
        const data = await this.getObject(s3Client, {
          Bucket: configuration.resource,
          Key: configuration.path
        });
        ret.output = data.Body.toString();
      } else if (s3Action === S3ActionType.DELETE_OBJECT) {
        if (!configuration.resource) {
          throw new IntegrationError('Resource required for delete objects');
        }
        if (!configuration.path) {
          throw new IntegrationError('Path required for delete objects');
        }
        await this.deleteObject(s3Client, {
          Bucket: configuration.resource,
          Key: configuration.path
        });
      } else if (s3Action === S3ActionType.UPLOAD_OBJECT) {
        if (!configuration.resource) {
          throw new IntegrationError('Resource required for upload objects');
        }
        if (!configuration.path) {
          throw new IntegrationError('Path required for upload objects');
        }
        const data = await this.upload(s3Client, {
          Bucket: configuration.resource,
          Key: configuration.path,
          Body: configuration.body
        });

        data.presignedURL = await this.generateSignedURL(s3Client, configuration.resource, configuration.path);
        ret.output = data;
      } else if (s3Action === S3ActionType.UPLOAD_MULTIPLE_OBJECTS) {
        if (!configuration.resource) {
          throw new IntegrationError('Resource required for uploading multiple objects');
        }
        if (!configuration.fileObjects) {
          throw new IntegrationError('File objects required for uploading multiple objects');
        }
        let filesWithContents = configuration.fileObjects;
        if (configuration.fileObjects && typeof configuration.fileObjects === 'string') {
          try {
            filesWithContents = JSON.parse(configuration.fileObjects);
          } catch (e) {
            throw new IntegrationError(`Can't parse the file objects. They must be an array of JSON objects.`);
          }
        }
        if (!Array.isArray(filesWithContents)) {
          throw new IntegrationError(`File objects must be an array of JSON objects.`);
        }

        const contents = filesWithContents.map((file: unknown) => {
          if (!isReadableFile(file)) {
            if (isReadableFileConstructor(file)) {
              return file.contents;
            }

            throw new IntegrationError('Cannot read files. Files can either be Superblocks files or { name: string; contents: string }.');
          }

          const match = (files as Array<RequestFile>).find((f) => f.filename === file.$superblocksId);
          if (!match) {
            throw new IntegrationError(`Could not locate file contents for file ${file.name}`);
          }
          // S3 supports streams as input, this is preferred to reading into memory for large files
          return fs.createReadStream(match.path);
        });

        const data = await this.uploadMultiple(
          s3Client,
          filesWithContents.map((file, i) => ({
            Bucket: configuration.resource,
            Key: file.name,
            Body: contents[i]
          }))
        );
        ret.output = await Promise.all(
          data.map(async (entry: { Key: string }) => ({
            ...entry,
            presignedURL: await this.generateSignedURL(s3Client, configuration.resource, entry.Key)
          }))
        );
      } else if (s3Action === S3ActionType.GENERATE_PRESIGNED_URL) {
        ret.output = await this.generateSignedURL(
          s3Client,
          configuration.resource,
          configuration.path,
          Number(configuration.custom?.presignedExpiration?.value)
        );
      }
      return ret;
    } catch (err) {
      throw new IntegrationError(`S3 request failed, ${err.message}`);
    }
  }

  getRequest(actionConfiguration: S3ActionConfiguration): RawRequest {
    const configuration = actionConfiguration;
    const s3Action = configuration.action;
    let s3ReqString = `Action: ${S3_ACTION_DISPLAY_NAMES[s3Action]}`;
    if (s3Action === S3ActionType.LIST_OBJECTS) {
      s3ReqString += `\nBucket: ${configuration.resource}`;
    } else if (s3Action === S3ActionType.GET_OBJECT) {
      s3ReqString += `\nBucket: ${configuration.resource}\nKey: ${JSON.stringify(configuration.path)}`;
    } else if (s3Action === S3ActionType.DELETE_OBJECT) {
      s3ReqString += `\nBucket: ${configuration.resource}\nKey: ${JSON.stringify(configuration.path)}`;
    } else if (s3Action === S3ActionType.UPLOAD_OBJECT) {
      s3ReqString += `\nBucket: ${configuration.resource}\nKey: ${JSON.stringify(configuration.path)}\nBody: ${configuration.body}`;
    } else if (s3Action === S3ActionType.UPLOAD_MULTIPLE_OBJECTS) {
      let files = configuration.fileObjects;
      if (configuration.fileObjects && typeof configuration.fileObjects === 'string') {
        try {
          files = JSON.parse(configuration.fileObjects);
        } catch (e) {
          throw new IntegrationError(`Can't parse the file objects. They must be an array of JSON objects.`);
        }
      }
      if (!Array.isArray(files)) {
        throw new IntegrationError(`File objects must be an array of JSON objects.`);
      }
      const names = files.map((file) => file.name);
      s3ReqString += `\nBucket: ${configuration.resource}\nFile Objects: ${JSON.stringify(names)}`;
    } else if (s3Action === S3ActionType.GENERATE_PRESIGNED_URL) {
      s3ReqString += `\nBucket: ${configuration.resource}\nKey: ${JSON.stringify(configuration.path)})}\nExpiration: ${
        configuration.custom?.presignedExpiration?.value
      }`;
    }
    return s3ReqString;
  }

  dynamicProperties(): string[] {
    return ['action', 'resource', 'path', 'body', 'fileObjects'];
  }

  async metadata(datasourceConfiguration: S3DatasourceConfiguration): Promise<DatasourceMetadataDto> {
    try {
      const s3Client = this.getS3Client(datasourceConfiguration);
      const data = await this.listBuckets(s3Client);
      return {
        buckets: data.Buckets.map((bucket) => ({
          name: bucket.Name
        }))
      };
    } catch (e) {
      this.logger.debug(`Failed to fetch buckets; expected that the credentials may be limited: ${e}`);
      return {};
    }
  }

  private getS3Client(datasourceConfig: S3DatasourceConfiguration): S3 {
    const s3Client = new S3(this.getAwsConfig(datasourceConfig));
    return s3Client;
  }

  private getAwsConfig(datasourceConfig: S3DatasourceConfiguration) {
    return {
      region: datasourceConfig.authentication?.custom?.region?.value,
      accessKeyId: datasourceConfig.authentication?.custom?.accessKeyID?.value,
      secretAccessKey: datasourceConfig.authentication?.custom?.secretKey?.value
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async listObjects(s3Client: S3, request: ListObjectsRequest): Promise<any> {
    return s3Client.listObjects(request).promise();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async listBuckets(s3Client: S3): Promise<any> {
    return s3Client.listBuckets().promise();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async getObject(s3Client: S3, request: GetObjectRequest): Promise<any> {
    return s3Client.getObject(request).promise();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async deleteObject(s3Client: S3, request: DeleteObjectRequest): Promise<any> {
    return s3Client.deleteObject(request).promise();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async upload(s3Client: S3, request: PutObjectRequest): Promise<any> {
    return s3Client.upload(request).promise();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async uploadMultiple(s3Client: S3, requests: PutObjectRequest[]): Promise<any> {
    return Promise.all(requests.map((r) => s3Client.upload(r).promise()));
  }

  private async generateSignedURL(s3Client: S3, bucket: string, key: string, expiration?: number): Promise<string> {
    const url = await s3Client.getSignedUrlPromise('getObject', {
      Bucket: bucket,
      Key: key,
      Expires: expiration ?? DEFAULT_S3_PRESIGNED_URL_EXPIRATION_SECONDS
    });
    return url;
  }

  async test(datasourceConfiguration: S3DatasourceConfiguration): Promise<void> {
    try {
      const stsClient = new STS(this.getAwsConfig(datasourceConfiguration));
      // This call will work with any valid AWS credentials, regardless of permissions
      // Ref: https://docs.aws.amazon.com/cli/latest/reference/sts/get-caller-identity.html
      await stsClient.getCallerIdentity().promise();
    } catch (err) {
      throw new IntegrationError(`S3 client configuration failed. ${err.message}`);
    }
  }
}

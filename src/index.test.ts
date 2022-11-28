import { S3ActionType } from '@superblocksteam/shared';
import { DUMMY_EXECUTE_COMMON_PARAMETERS } from '@superblocksteam/shared-backend';
import { S3 } from 'aws-sdk';
import S3Plugin from '.';

const DUMMY_ZIP_FILE = {
  destination: '/temp-folder/dev-agent-key',
  fieldname: 'files',
  filename: 'uppy-superblocks_master_zip-1d-1e-application_zip-11343326-1669048984124_dev-agent-key',
  mimetype: 'application/zip',
  originalname: 'uppy-superblocks_master_zip-1d-1e-application_zip-11343326-1669048984124',
  path: '/temp-folder/dev-agent-key/uppy-superblocks_master_zip-1d-1e-application_zip-11343326-1669048984124_dev-agent-key'
};

const DUMMY_ZIP_FILE_OBJECT = {
  name: 'superblocks-master.zip',
  extension: 'zip',
  type: 'application/zip',
  size: 1005,
  encoding: 'text',
  $superblocksId: 'uppy-superblocks_master_zip-1d-1e-application_zip-11343326-1669048984124'
};

jest.mock('aws-sdk');

jest.mock('@superblocksteam/shared-backend', () => {
  const originalModule = jest.requireActual('@superblocksteam/shared-backend');
  return {
    __esModule: true,
    ...originalModule,
    getFileStream: jest.fn((context, location) => {
      return 'some content';
    })
  };
});

describe('s3 upload', () => {
  beforeAll(() => {
    jest.spyOn(S3.prototype, 'upload').mockImplementation(() => {
      return {
        abort: () => {
          // do nothing
        },
        promise: (): Promise<S3.ManagedUpload.SendData> => {
          return new Promise((resolve) => {
            resolve({
              Location: 'https://fancy-bucket.s3.amazonaws.com/superblocks-master.zip',
              ETag: '"123123123XYxyzabc123abcXYZABCDAB"',
              Bucket: 'fancy-bucket',
              Key: 'superblocks-master.zip'
            });
          });
        },
        send: () => {
          // do nothing
        },
        on: (event, listener) => {
          // do nothing
        }
      };
    });
  });

  const plugin: S3Plugin = new S3Plugin();
  test('uploading single file, happy path scenario', async () => {
    const uploadObjectResult = await plugin.execute({
      ...DUMMY_EXECUTE_COMMON_PARAMETERS,
      datasourceConfiguration: {},
      actionConfiguration: {
        action: S3ActionType.UPLOAD_OBJECT,
        resource: 'fancy-bucket',
        path: 'superblocks-master.zip',
        body: 'some content'
      },
      files: [DUMMY_ZIP_FILE]
    });
    expect(uploadObjectResult).toEqual({
      log: [],
      output: {
        Bucket: 'fancy-bucket',
        ETag: '"123123123XYxyzabc123abcXYZABCDAB"',
        Key: 'superblocks-master.zip',
        Location: 'https://fancy-bucket.s3.amazonaws.com/superblocks-master.zip'
      }
    });
    expect(S3.prototype.upload).toBeCalledWith({
      Body: 'some content',
      Bucket: 'fancy-bucket',
      ContentType: 'application/zip',
      Key: 'superblocks-master.zip'
    });
  });
  test('uploading multiple files, happy path scenario', async () => {
    const uploadMulitpleResult = await plugin.execute({
      ...DUMMY_EXECUTE_COMMON_PARAMETERS,
      datasourceConfiguration: {},
      actionConfiguration: {
        action: S3ActionType.UPLOAD_MULTIPLE_OBJECTS,
        resource: 'fancy-bucket',
        fileObjects: JSON.stringify([DUMMY_ZIP_FILE_OBJECT])
      },
      files: [DUMMY_ZIP_FILE]
    });
    expect(uploadMulitpleResult).toEqual({
      log: [],
      output: [
        {
          Bucket: 'fancy-bucket',
          ETag: '"123123123XYxyzabc123abcXYZABCDAB"',
          Key: 'superblocks-master.zip',
          Location: 'https://fancy-bucket.s3.amazonaws.com/superblocks-master.zip'
        }
      ]
    });
    expect(S3.prototype.upload).toBeCalledWith({
      Body: 'some content',
      Bucket: 'fancy-bucket',
      ContentType: 'application/zip',
      Key: 'superblocks-master.zip'
    });
  });
});

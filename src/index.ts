/**
 * File: index.ts
 * Project: aws-nodejs
 * File Created: Thursday, 19th October 2023 12:31:56 pm
 * Author: José Compadre Junior (jose.compadre@nimbleevolution.com)
 * -----
 * Last Modified: Saturday, 18th November 2023 12:31:43 pm
 * Modified By: José Compadre Junior (jose.compadre@nimbleevolution.com>)
 * -----
 * Copyright (c) 2023 Nimble Evolution, Nimble Evolution. All rights reserved.
 */

import {
  ListBucketsCommand,
  S3Client,
  SelectObjectContentCommand,
  SelectObjectContentCommandInput,
  SelectObjectContentEventStream
} from '@aws-sdk/client-s3';

/**
 * Search content by a given query
 * @param bucketName name of the bucket to search
 * @param key name of the file to query
 * @param query the query to be sent
 * @returns a string with the content
 */
const getBucketContentByQuery = async (
  bucketName: string,
  key: string,
  query: string
) => {
  const client = new S3Client({});
  const command = new ListBucketsCommand({});

  try {
    const { Owner, Buckets } = await client.send(command);
    console.log(
      `${Owner?.DisplayName} owns ${Buckets?.length} bucket${
        Buckets?.length === 1 ? '' : 's'
      }:`
    );
    console.log(`${Buckets?.map((b) => ` • ${b.Name}`).join('\n')}`);

    const input: SelectObjectContentCommandInput = {
      // SelectObjectContentRequest
      Bucket: bucketName, // required
      Key: key, // required
      Expression: query, // required
      ExpressionType: 'SQL', // required
      RequestProgress: {
        // RequestProgress
        Enabled: true || false
      },
      InputSerialization: {
        // InputSerialization
        CompressionType: 'NONE',
        JSON: {
          // JSONInput
          Type: 'LINES'
        }
      },
      OutputSerialization: {
        // OutputSerialization
        JSON: {
          // JSONOutput
          RecordDelimiter: '\n'
        }
      }
    };
    const sqlCommand = new SelectObjectContentCommand(input);
    const response = await client.send(sqlCommand);
    const asyncIterableStreamToString = (
      asyncIterable: AsyncIterable<SelectObjectContentEventStream>
    ) =>
      new Promise<string>(async (resolve, reject) => {
        try {
          const chunks = [new Uint8Array()];
          for await (const selectObjectContentEventStream of asyncIterable) {
            if (selectObjectContentEventStream.Records) {
              if (selectObjectContentEventStream.Records.Payload) {
                chunks.push(selectObjectContentEventStream.Records.Payload);
              }
            }

            if (selectObjectContentEventStream.End) {
              resolve(Buffer.concat(chunks).toString('utf8'));
            }
          }
        } catch (err) {
          console.error(err);
          reject();
        }
      });
    if (response.$metadata.httpStatusCode == 200) {
      if (response.Payload) {
        const body = await asyncIterableStreamToString(response.Payload);
        return body;
      } else {
        console.warn(`S3Select did not have payload for ${bucketName}/${key}`);
        return undefined;
      }
    } else {
      var str = JSON.stringify(response.$metadata);
      console.warn(
        `S3Select did not receive 200 for ${bucketName}/${key}. Metadata is ${str}`
      );
      return undefined;
    }
  } catch (err) {
    console.error(err);
  }
};

export const main = async () => {
  const bucketName = 'dock-zendesk-ticket-import';
  const key = 'dock_test_tickets_sample.json';
  const query = 'SELECT * FROM s3object s LIMIT 5';
  const body = await getBucketContentByQuery(bucketName, key, query);
  if (body) {
    const strArray = body.split('\n');
    //console.log(strArray);
    strArray.map((item) => {
      try {
        const obj = JSON.parse(item);
        if (obj) console.log(obj);
        return true;
      } catch (err) {
        console.log(`Invalid JSON input: ${item}`);
      }
    });
  }
};

main();

'use strict'; //eslint-disable-line

const assert = require('assert');
const http = require('http');

const Logger = require('werelogs').Logger;

const { RecordLogProxy } = require(
    '../../../../../lib/storage/metadata/bucketclient/RecordLog.js');

/* eslint-disable max-len */
const mockedLogResponse = `
    [
        "{\\"db\\":\\"funbucket\\",\\"entries\\":[{\\"key\\":\\"coolkey\\",\\"value\\":\\"{\\\\\\"md-model-version\\\\\\":2,\\\\\\"owner-display-name\\\\\\":\\\\\\"Bart\\\\\\",\\\\\\"owner-id\\\\\\":\\\\\\"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be\\\\\\",\\\\\\"content-length\\\\\\":174,\\\\\\"content-type\\\\\\":\\\\\\"text/plain\\\\\\",\\\\\\"last-modified\\\\\\":\\\\\\"2017-05-24T23:54:59.837Z\\\\\\",\\\\\\"content-md5\\\\\\":\\\\\\"875fbeafc5de4e9025ad844f7321f5bd\\\\\\",\\\\\\"x-amz-server-version-id\\\\\\":\\\\\\"\\\\\\",\\\\\\"x-amz-storage-class\\\\\\":\\\\\\"STANDARD\\\\\\",\\\\\\"x-amz-server-side-encryption\\\\\\":\\\\\\"\\\\\\",\\\\\\"x-amz-server-side-encryption-aws-kms-key-id\\\\\\":\\\\\\"\\\\\\",\\\\\\"x-amz-server-side-encryption-customer-algorithm\\\\\\":\\\\\\"\\\\\\",\\\\\\"location\\\\\\":[{\\\\\\"key\\\\\\":\\\\\\"088bac1ca870821d154feb15f0325dbd223e629b\\\\\\",\\\\\\"size\\\\\\":174,\\\\\\"start\\\\\\":0,\\\\\\"dataStoreName\\\\\\":\\\\\\"file\\\\\\"}],\\\\\\"acl\\\\\\":{\\\\\\"Canned\\\\\\":\\\\\\"private\\\\\\",\\\\\\"FULL_CONTROL\\\\\\":[],\\\\\\"WRITE_ACP\\\\\\":[],\\\\\\"READ\\\\\\":[],\\\\\\"READ_ACP\\\\\\":[]},\\\\\\"isDeleteMarker\\\\\\":false,\\\\\\"x-amz-meta-s3cmd-attrs\\\\\\":\\\\\\"uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1495045891/atime:1495669184/md5:875fbeafc5de4e9025ad844f7321f5bd/ctime:1495045891\\\\\\"}\\"}]}",
        "{\\"db\\":\\"funbucket\\",\\"entries\\":[{\\"key\\":\\"coolkey\\",\\"type\\":\\"del\\"}]}"
    ]
`;
/* eslint-enable max-len */

describe('raft record log client', () => {
    let server;
    const cliLogger = new Logger('recordLog:test-client',
                                 { level: 'info', dump: 'error' });
    let logClient;

    function setup(done) {
        // mock a raft admin server
        server = http.createServer((req, res) => {
            res.writeHead(200);
            res.end(mockedLogResponse);
        });
        server.listen(6677);

        logClient = new RecordLogProxy({
            url: 'http://localhost:6677/mockLog', logger: cliLogger
        });
        done();
    }

    before(done => {
        setup(done);
    });

    after(done => {
        server.close();
        done();
    });

    describe('readRecords', () => {
        it('should list all records in a log', done => {
            const recordStream = logClient.readRecords();
            let nbRecords = 0;
            recordStream.on('data', data => {
                assert.strictEqual(data.db, 'funbucket');
                assert.strictEqual(data.entries.length, 1);
                const entry = data.entries[0];
                if (nbRecords === 0) {
                    assert.strictEqual(entry.type, 'put');
                    assert.strictEqual(entry.key, 'coolkey');
                    assert(entry.value.length > 0);
                } else {
                    assert.strictEqual(nbRecords, 1);
                    assert.strictEqual(entry.type, 'del');
                    assert.strictEqual(entry.key, 'coolkey');
                    assert.strictEqual(entry.value, undefined);
                }
                nbRecords += 1;
            });
            recordStream.on('end', () => {
                assert.strictEqual(nbRecords, 2);
                done();
            });
        });
    });
});

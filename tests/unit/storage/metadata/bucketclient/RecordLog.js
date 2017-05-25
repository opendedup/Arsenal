'use strict'; //eslint-disable-line

const assert = require('assert');
const http = require('http');

const Logger = require('werelogs').Logger;

const { openLog } = require(
    '../../../../../lib/storage/metadata/bucketclient/RecordLog.js');

function randomName() {
    let text = '';
    const possible = ('ABCDEFGHIJKLMNOPQRSTUVWXYZ' +
                      'abcdefghijklmnopqrstuvwxyz0123456789');

    for (let i = 0; i < 5; i++) {
        text += possible.charAt(Math.floor(Math.random()
                                           * possible.length));
    }
    return text;
}

function createScratchRecordLog(logClient, done) {
    const name = randomName();
    debug(`creating scratch log ${name}`);
    logClient.openLog(name, (err, proxy) => {
        assert.ifError(err);
        assert(proxy);
        done(null, proxy);
    });
}

function closeRecordLog(openLog, done) {
    debug(`closing scratch log ${openLog.name}`);
    openLog.on('disconnect', done);
    openLog.disconnect();
}

const mockedLogResponse = `
    [
        {
            "db": "funbucket",
            "entries": [
                {
                    "key": "coolkey",
                    "value": "{\\"md-model-version\\":2,\\"owner-display-name\\":\\"Bart\\",\\"owner-id\\":\\"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be\\",\\"content-length\\":174,\\"content-type\\":\\"text/plain\\",\\"last-modified\\":\\"2017-05-24T23:54:59.837Z\\",\\"content-md5\\":\\"875fbeafc5de4e9025ad844f7321f5bd\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"STANDARD\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"location\\":[{\\"key\\":\\"088bac1ca870821d154feb15f0325dbd223e629b\\",\\"size\\":174,\\"start\\":0,\\"dataStoreName\\":\\"file\\"}],\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"isDeleteMarker\\":false,\\"x-amz-meta-s3cmd-attrs\\":\\"uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1495045891/atime:1495669184/md5:875fbeafc5de4e9025ad844f7321f5bd/ctime:1495045891\\"}"
                }
            ],
            "seq": 5,
            "timestamp": "2017-05-24T23:54:59.853Z"
        },
        {
            "db": "funbucket",
            "entries": [
                {
                    "key": "coolkey",
                    "type": "del"
                }
            ],
            "seq": 6,
            "timestamp": "2017-05-25T00:04:55.091Z"
        }
    ]
`;

describe.only('raft record log client', () => {
    let server;
    const cliLogger = new Logger('recordLog:test-client',
                                 { level: 'info', dump: 'error' });
    let db;
    let logClient;

    function setup(done) {
        // mock a raft admin server
        server = http.createServer((req, res) => {
            res.writeHead(200);
            res.end(mockedLogResponse);
        });
        server.listen(6677);

        openLog({ url: 'http://localhost:6677',
                  logger: cliLogger }, (err, logProxy) => {
                      logClient = logProxy;
                      done();
                  });
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
                    assert.strictEqual(data.seq, 5);
                    assert.strictEqual(entry.type, 'put');
                    assert.strictEqual(entry.key, 'coolkey');
                    assert(entry.value.length > 0);
                } else {
                    assert.strictEqual(nbRecords, 1);
                    assert.strictEqual(data.seq, 6);
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

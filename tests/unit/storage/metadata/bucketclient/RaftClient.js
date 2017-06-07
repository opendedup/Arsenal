'use strict'; //eslint-disable-line

const assert = require('assert');

const RaftClient = require(
    '../../../../../lib/storage/metadata/bucketclient/RaftClient.js');

/* eslint-disable max-len */
const mockedLogResponse = `{
    "info": { "start": 10, "end": 11, "prune": 10, "cseq": 11 },
    "log": [
        {
            "db": "funbucket",
            "entries": [
                {
                    "key": "coolkey0",
                    "value": "{\\"md-model-version\\":2,\\"owner-display-name\\":\\"test_1497565858\\",\\"owner-id\\":\\"cdb8f647bb447783ade4371725f98bd5768ed56f82359c8bcba48a973b47d897\\",\\"content-length\\":542,\\"content-type\\":\\"text/plain\\",\\"last-modified\\":\\"2017-06-16T22:37:20.466Z\\",\\"content-md5\\":\\"01064f35c238bd2b785e34508c3d27f4\\",\\"x-amz-version-id\\":\\"null\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"STANDARD\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"x-amz-website-redirect-location\\":\\"\\",\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"key\\":\\"\\",\\"location\\":[{\\"key\\":\\"1B2A0751D286B0F3C235ED226404F959C12BB600\\",\\"size\\":542,\\"start\\":0,\\"dataStoreName\\":\\"us-east-1\\",\\"dataStoreType\\":\\"scality\\"}],\\"isDeleteMarker\\":false,\\"tags\\":{},\\"replicationInfo\\":{\\"status\\":\\"PENDING\\",\\"content\\":[\\"DATA\\",\\"METADATA\\"],\\"destination\\":\\"arn:aws:s3:::destbucket\\",\\"storageClass\\":\\"STANDARD\\"},\\"x-amz-meta-s3cmd-attrs\\":\\"uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1490807629/atime:1497634270/md5:01064f35c238bd2b785e34508c3d27f4/ctime:1490807629\\",\\"versionId\\":\\"98502347359531999999RG001  5.2375.2122\\"}"
                },
                {
                    "key": "coolkey0\\u000098502347359531999999RG001  5.2375.2122",
                    "value": "{\\"md-model-version\\":2,\\"owner-display-name\\":\\"test_1497565858\\",\\"owner-id\\":\\"cdb8f647bb447783ade4371725f98bd5768ed56f82359c8bcba48a973b47d897\\",\\"content-length\\":542,\\"content-type\\":\\"text/plain\\",\\"last-modified\\":\\"2017-06-16T22:37:20.466Z\\",\\"content-md5\\":\\"01064f35c238bd2b785e34508c3d27f4\\",\\"x-amz-version-id\\":\\"null\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"STANDARD\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"x-amz-website-redirect-location\\":\\"\\",\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"key\\":\\"\\",\\"location\\":[{\\"key\\":\\"1B2A0751D286B0F3C235ED226404F959C12BB600\\",\\"size\\":542,\\"start\\":0,\\"dataStoreName\\":\\"us-east-1\\",\\"dataStoreType\\":\\"scality\\"}],\\"isDeleteMarker\\":false,\\"tags\\":{},\\"replicationInfo\\":{\\"status\\":\\"PENDING\\",\\"content\\":[\\"DATA\\",\\"METADATA\\"],\\"destination\\":\\"arn:aws:s3:::destbucket\\",\\"storageClass\\":\\"STANDARD\\"},\\"x-amz-meta-s3cmd-attrs\\":\\"uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1490807629/atime:1497634270/md5:01064f35c238bd2b785e34508c3d27f4/ctime:1490807629\\",\\"versionId\\":\\"98502347359531999999RG001  5.2375.2122\\"}"
                }
            ],
            "method": 8
        },
        {
            "db": "funbucket",
            "entries": [
                {
                    "key": "coolkey0",
                    "value": "{\\"md-model-version\\":2,\\"owner-display-name\\":\\"test_1497565858\\",\\"owner-id\\":\\"cdb8f647bb447783ade4371725f98bd5768ed56f82359c8bcba48a973b47d897\\",\\"content-length\\":0,\\"last-modified\\":\\"2017-06-16T22:37:43.985Z\\",\\"content-md5\\":\\"d41d8cd98f00b204e9800998ecf8427e\\",\\"x-amz-version-id\\":\\"null\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"STANDARD\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"x-amz-website-redirect-location\\":\\"\\",\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"key\\":\\"\\",\\"location\\":null,\\"isDeleteMarker\\":true,\\"tags\\":{},\\"replicationInfo\\":{\\"status\\":\\"PENDING\\",\\"content\\":[\\"METADATA\\"],\\"destination\\":\\"arn:aws:s3:::destbucket\\",\\"storageClass\\":\\"STANDARD\\"},\\"versionId\\":\\"98502347336011999999RG001  5.2376.2123\\"}"
                },
                {
                    "key": "coolkey0\\u000098502347336011999999RG001  5.2376.2123",
                    "value": "{\\"md-model-version\\":2,\\"owner-display-name\\":\\"test_1497565858\\",\\"owner-id\\":\\"cdb8f647bb447783ade4371725f98bd5768ed56f82359c8bcba48a973b47d897\\",\\"content-length\\":0,\\"last-modified\\":\\"2017-06-16T22:37:43.985Z\\",\\"content-md5\\":\\"d41d8cd98f00b204e9800998ecf8427e\\",\\"x-amz-version-id\\":\\"null\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"STANDARD\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"x-amz-website-redirect-location\\":\\"\\",\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"key\\":\\"\\",\\"location\\":null,\\"isDeleteMarker\\":true,\\"tags\\":{},\\"replicationInfo\\":{\\"status\\":\\"PENDING\\",\\"content\\":[\\"METADATA\\"],\\"destination\\":\\"arn:aws:s3:::destbucket\\",\\"storageClass\\":\\"STANDARD\\"},\\"versionId\\":\\"98502347336011999999RG001  5.2376.2123\\"}"
                }
            ],
            "method": 8
        }
    ]
}`;
/* eslint-enable max-len */

// mock a simple bucketclient to get a fake raft log
class BucketClientMock {

    getRaftLog(raftId, start, limit, targetLeader, reqUids,
               callback) {
        process.nextTick(() => callback(null, mockedLogResponse));
    }
}

describe('raft record log client', () => {
    let logClient;
    let logProxy;

    function setup(done) {
        const bucketClient = new BucketClientMock();
        logClient = new RaftClient({ bucketClient });
        logProxy = logClient.openRecordLog(0);
        done();
    }

    before(done => {
        setup(done);
    });

    after(done => {
        done();
    });

    describe('readRecords', () => {
        it('should list all records in a log', done => {
            let nbRecords = 0;
            logProxy.readRecords({}, (err, info) => {
                const recordStream = info.log;
                recordStream.on('data', data => {
                    assert.strictEqual(data.db, 'funbucket');
                    assert.strictEqual(data.entries.length, 2);
                    const entry = data.entries[1];
                    if (nbRecords === 0) {
                        assert.strictEqual(entry.type, 'put');
                        assert.strictEqual(
                            entry.key,
                            ('coolkey0\u000098502347359531999999RG001  ' +
                             '5.2375.2122'));
                        assert(entry.value.length > 0);
                    } else {
                        assert.strictEqual(nbRecords, 1);
                        assert.strictEqual(entry.type, 'put');
                        assert.strictEqual(
                            entry.key,
                            ('coolkey0\u000098502347336011999999RG001  ' +
                             '5.2376.2123'));
                        assert(entry.value.length > 0);
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
});

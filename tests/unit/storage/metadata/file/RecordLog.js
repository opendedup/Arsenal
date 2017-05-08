'use strict'; //eslint-disable-line

const assert = require('assert');
const temp = require('temp');
const debug = require('debug')('record-log:test');
const level = require('level');
const sublevel = require('level-sublevel');

const Logger = require('werelogs').Logger;

const rpc = require('../../../../../lib/network/rpc/rpc');
const { RecordLogClient, RecordLogService } =
          require('../../../../../lib/storage/metadata/file/RecordLog.js');

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

describe('record log - persistent log of metadata operations', () => {
    let server;
    const srvLogger = new Logger('recordLog:test-server',
                                 { level: 'info', dump: 'error' });
    const cliLogger = new Logger('recordLog:test-client',
                                 { level: 'info', dump: 'error' });
    let db;
    let logClient;

    function setup(done) {
        server = new rpc.RPCServer({ logger: srvLogger });
        server.listen(6677);

        new RecordLogService({ // eslint-disable-line no-new
            server,
            namespace: '/test/recordLog',
            logger: srvLogger,
            rootDb: db,
        });
        logClient = new RecordLogClient({
            url: 'http://localhost:6677/test/recordLog',
            logger: cliLogger,
        });
        done();
    }

    before(done => {
        temp.mkdir('record-log-testdir-', (err, dbDir) => {
            const rootDb = level(dbDir);
            db = sublevel(rootDb);
            setup(done);
        });
    });

    after(done => {
        server.close();
        done();
    });

    describe('simple tests', () => {
        // reinitialized for each test
        let logProxy;

        beforeEach(done => {
            createScratchRecordLog(logClient, (err, proxy) => {
                logProxy = proxy;
                done();
            });
        });
        afterEach(done => {
            if (logProxy) {
                closeRecordLog(logProxy, () => {
                    logProxy = undefined;
                    done();
                });
            } else {
                done();
            }
        });

        it('should be able to add records and list them thereafter', done => {
            debug('going to append records');
            const ops = [{ type: 'put', key: 'foo', value: 'bar',
                           prefix: ['foobucket'] },
                         { type: 'del', key: 'baz',
                           prefix: ['foobucket'] },
                         { type: 'put',
                           key: 'Pâtisserie=中文-español-English',
                           value: 'yummy',
                           prefix: ['foobucket'] },
                        ];
            logProxy.createLogRecordOps(ops, (err, logEntries) => {
                assert.ifError(err);
                db.batch(ops.concat(logEntries), err => {
                    assert.ifError(err);
                    logProxy.readRecords({}, (err, recordStream) => {
                        assert.ifError(err);
                        assert(recordStream);
                        debug('readRecords: received new recordStream');
                        let nbRecords = 0;
                        recordStream.on('data', record => {
                            debug('readRecords: next record:', record);
                            if (nbRecords === 0) {
                                assert.strictEqual(record.seq, 1);
                                assert.strictEqual(record.type, 'put');
                                assert.strictEqual(record.key, 'foo');
                                assert.strictEqual(record.value, 'bar');
                                assert.deepStrictEqual(record.prefix,
                                                       ['foobucket']);
                                assert.strictEqual(typeof record.timestamp,
                                                   'string');
                            } else if (nbRecords === 1) {
                                assert.strictEqual(record.seq, 2);
                                assert.strictEqual(record.type, 'del');
                                assert.strictEqual(record.key, 'baz');
                                assert.strictEqual(record.value, undefined);
                                assert.deepStrictEqual(record.prefix,
                                                       ['foobucket']);
                                assert.strictEqual(typeof record.timestamp,
                                                   'string');
                            } else if (nbRecords === 2) {
                                assert.strictEqual(record.seq, 3);
                                assert.strictEqual(record.type, 'put');
                                assert.strictEqual(
                                    record.key,
                                    'Pâtisserie=中文-español-English');
                                assert.strictEqual(record.value, 'yummy');
                                assert.deepStrictEqual(record.prefix,
                                                       ['foobucket']);
                                assert.strictEqual(typeof record.timestamp,
                                                   'string');
                            }
                            nbRecords += 1;
                        });
                        recordStream.on('end', () => {
                            debug('readRecords: stream end');
                            assert.strictEqual(nbRecords, 3);
                            done();
                        });
                    });
                });
            });
        });
    });

    describe('readRecords', () => {
        let logProxy;

        before(done => {
            createScratchRecordLog(logClient, (err, proxy) => {
                logProxy = proxy;
                // fill the log with 1000 entries
                debug('going to append records');
                const recordsToAdd = [];
                for (let i = 1; i <= 1000; ++i) {
                    recordsToAdd.push(
                        { type: 'put', key: `foo${i}`, value: `bar${i}`,
                          prefix: ['foobucket'] });
                }
                logProxy.createLogRecordOps(recordsToAdd, (err, logRecs) => {
                    assert.ifError(err);
                    db.batch(recordsToAdd.concat(logRecs), err => {
                        assert.ifError(err);
                        done();
                    });
                });
            });
        });

        function checkRecord(record, seq) {
            assert.strictEqual(record.seq, seq);
            assert.strictEqual(record.type, 'put');
            assert.strictEqual(record.key, `foo${seq}`);
            assert.strictEqual(record.value, `bar${seq}`);
            assert.deepStrictEqual(record.prefix, ['foobucket']);
            assert.strictEqual(typeof record.timestamp, 'string');
        }
        function checkRecordStream(recordStream, params, done) {
            assert(recordStream);
            debug('readRecords: received new recordStream');
            let seq = params.startSeq;
            recordStream.on('data', record => {
                debug('readRecords: next record:', record);
                checkRecord(record, seq);
                seq += 1;
            });
            recordStream.on('end', () => {
                debug('readRecords: stream end');
                assert.strictEqual(seq, params.endSeq + 1);
                done();
            });
        }
        it('should list all entries', done => {
            logProxy.readRecords({}, (err, recordStream) => {
                assert.ifError(err);
                checkRecordStream(recordStream,
                                  { startSeq: 1, endSeq: 1000 }, done);
            });
        });

        it('should list all entries from a given minSeq', done => {
            logProxy.readRecords({ minSeq: 500 }, (err, recordStream) => {
                assert.ifError(err);
                checkRecordStream(recordStream,
                                  { startSeq: 500, endSeq: 1000 }, done);
            });
        });

        it('should list all entries up to a given maxSeq', done => {
            logProxy.readRecords({ maxSeq: 500 }, (err, recordStream) => {
                assert.ifError(err);
                checkRecordStream(recordStream,
                                  { startSeq: 1, endSeq: 500 }, done);
            });
        });

        it('should list all entries in a seq range', done => {
            logProxy.readRecords(
                { minSeq: 100, maxSeq: 500 }, (err, recordStream) => {
                    assert.ifError(err);
                    checkRecordStream(recordStream,
                                      { startSeq: 100, endSeq: 500 }, done);
                });
        });

        it('should list all entries from a given minSeq up to a limit',
        done => {
            logProxy.readRecords(
                { minSeq: 100, limit: 100 },
                (err, recordStream) => {
                    assert.ifError(err);
                    checkRecordStream(recordStream,
                                      { startSeq: 100, endSeq: 199 }, done);
                });
        });
    });
});

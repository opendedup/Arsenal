'use strict'; // eslint-disable-line

const stream = require('stream');
const debug = require('debug')('record-log');

const errors = require('../../../errors');
const rpc = require('../../../network/rpc/rpc');

const RECORD_LOG_PREFIX = '..recordLogs';

/**
 * @class
 * @classdesc Object returned by a call to {@link RecordLogClient#openLog}
 * to access a new or existing record log on a metadata daemon
 *
 * The provided RPC API is the one exposed by the {@link RecordLogService}
 * object.
 */
class RecordLogProxy extends rpc.BaseClient {

    constructor(params) {
        super(params);

        this.name = params.name;
        this.addRequestInfoProducer(
            logProxy => ({ logName: logProxy.name }));
    }
}


/**
 * Open a new or existing record log and access its API through RPC
 * calls.
 *
 * @param {Object} params - openLog params
 * @param {String} params.url - socket.io URL to the record log service
 * @param {Logger} params.logger - logger object
 * @param {String} [params.logName] - name of log to open (default "main")
 * @param {Function} done - callback expecting an error argument, or
 *   null and the opened log proxy object on success
 * @return {undefined}
 */
function openLog(params, done) {
    const logProxy = new RecordLogProxy({
        url: params.url,
        name: params.logName,
        logger: params.logger,
    });
    logProxy.connect(err => {
        if (err) {
            done(err);
        } else {
            done(null, logProxy);
        }
    });
}

function formatSeq(seq) {
    if (seq === undefined) {
        return undefined;
    }
    return `0000000000000000${seq.toString()}`.slice(-16);
}

function formatLogRecord(dbOp) {
    const timestamp = new Date().toJSON();
    return JSON.stringify({
        type: dbOp.type || 'put',
        db: dbOp.prefix[0],
        key: dbOp.key,
        value: dbOp.value,
        timestamp,
    });
}

class ListRecordStream extends stream.Transform {
    constructor() {
        super({ objectMode: true });
    }

    _transform(dbRecord, encoding, callback) {
        debug('transforming db record:', dbRecord);
        const logRecord = JSON.parse(dbRecord.value);
        const streamObject = {
            seq: Number.parseInt(dbRecord.key, 10),
            timestamp: logRecord.timestamp,
            db: logRecord.db,
            // only one entry, but keep format compatible with MetaData
            entries: [
                {
                    type: logRecord.type,
                    key: logRecord.key,
                    value: logRecord.value,
                },
            ],
        };
        callback(null, streamObject);
    }
}

/**
 * @class
 * @classdesc Expose an RPC API to access a record log
 *
 * The record log is stored as part of the metadata LevelDB database,
 * in a separate sub-level prefix, so that metadata and record log
 * updates can be transactional with each other.
 */
class RecordLogService extends rpc.BaseService {

    /**
     * @constructor
     *
     * @param {Object} params - constructor params
     * @param {String} params.namespace - socket.io namespace, a free
     *   string name that must start with '/'. The client will have to
     *   provide the same namespace in the URL
     *   (http://host:port/namespace)
     * @param {level-sublevel} params.rootDb - root LevelDB metadata
     *   database object (as returned by "level-sublevel" module)
     * @param {Object} params.logger - logger object
     * @param {RPCServer} [params.server] - convenience parameter,
     * calls server.registerServices() automatically
     *
     * The stored records, when read back by <tt>readRecords</tt>
     * contain the original batch update fields, plus the following
     * fields:
     *
     * - <tt>prefix</tt>: prefix in the database, aka. sub-level,
     *   as an Array (for bucketfile S3, it's a one-element array
     *   containing the bucket name)
     * - <tt>timestamp</tt>: timestamp of the operation when logged,
     *   for information purpose (e.g. '2017-05-03T00:00:34.808Z')
     * - <tt>seq</tt>: sequence number, starting from 1 and
     *   monotonically increasing for each logged update, as a Number
     */
    constructor(params) {
        super(params);

        this.openedRecordLogs = {};
        this.rootLog = params.rootDb.sublevel(RECORD_LOG_PREFIX);
        this.addRequestInfoConsumer((logService, reqParams) => {
            const env = {};
            if (reqParams.logName) {
                let openLog = this.openedRecordLogs[reqParams.logName];
                if (!openLog) {
                    openLog = {
                        logName: reqParams.logName,
                        logDb: this.rootLog.sublevel(reqParams.logName),
                        // next seq will be set to the highest seq in
                        // the DB + 1 in API commands
                        seq: undefined,
                    };
                    this.openedRecordLogs[reqParams.logName] = openLog;
                }
                env.openLog = openLog;
            }
            return env;
        });

        this.registerAsyncAPI({
            /**
             * Generate a series of record log operations as a batch
             * array to append to a LevelDB batch, from the array of
             * original batch LevelDB operations in
             * <tt>recordList</tt>.
             *
             * @note This call is asynchronous because
             * it may fetch the latest sequence number from the db
             * when first called (the subsequent calls will use the
             * cached value).
             *
             * @param {Object} env - Request environment passed by the
             *   RPC service
             * @param {Object} env.openLog - Info about the target
             *   record log
             * @param {Array} dbOps - Array of LevelDB batch
             *   operations to log
             * @param {Function} cb - callback when done, called with
             *   an error argument on error, or <tt>null</tt> on success
             *   and the array of log updates to add to the global batch
             *   as second argument.
             * @return {undefined}
             *
             * Note that this function generates the next sequence
             * numbers for the log batch, so the batches should be
             * committed to the DB in the same order than the calls to
             * <tt>createLogRecordOps</tt> are made to keep sequence
             * numbers consistent and in sync with the logged
             * operations.
             *
             * In case the later batch fails during commit, this series
             * of sequence numbers will never make it to the log, so there
             * will be a gap in the logged sequence numbers.
             *
             * @example
             * <tt>createLogRecordOps({ openLog },
             *         [{ type: 'put', key: 'foo', value: 'bar' },
             *          { type: 'del', key: 'baz' }],
             *         (err, logOps) => { })</tt>
             */
            createLogRecordOps(env, dbOps, cb) {
                const { openLog } = env;
                this._findNextSeq(openLog, () => {
                    cb(null, dbOps.map(op => {
                        const seqKey = formatSeq(openLog.nextSeq);
                        openLog.nextSeq += 1;
                        const value = formatLogRecord(op);
                        return {
                            type: 'put',
                            prefix: [RECORD_LOG_PREFIX, openLog.logName],
                            key: seqKey, value,
                        };
                    }));
                });
            },

            /**
             * Prune the oldest records in the record log
             *
             * Note: not implemented yet
             *
             * @param {Object} env - Request environment passed by the
             *   RPC service, skipped in the client call
             * @param {Object} env.openLog - Info about the target
             *   record log
             * @param {Object} params - params object
             * @param {Function} cb - callback when done
             * @return {undefined}
             */
            pruneRecords: (env, params, cb) => {
                cb(errors.NotImplemented);
            },
        });
        this.registerSyncAPI({
            /**
             * Read a range of records from the log
             *
             * @param {Object} env - Request environment passed by the
             *   RPC service, skipped in the client call
             * @param {Object} env.openLog - Info about the target
             *   record log
             * @param {Object} [params] - params object
             * @param {Number} [params.startSeq] - read from this
             *   sequence number
             * @param {Number} [params.endSeq] - read up to this
             *   sequence number
             * @param {Number} [params.limit] - limit the number of
             *   records returned by the stream
             * @return {stream.Readable} stream of log record objects
             */
            readRecords: (env, params) => {
                const { openLog } = env;
                const _params = params || {};
                const queryParams = {
                    gte: formatSeq(_params.startSeq),
                    lte: formatSeq(_params.endSeq),
                    limit: _params.limit,
                };
                const userStream = new ListRecordStream();
                const dbStream = openLog.logDb.createReadStream(queryParams);
                dbStream.pipe(userStream);
                return userStream;
            },
        });
    }

    _findNextSeq(openLog, cb) {
        if (openLog.nextSeq !== undefined) {
            return setImmediate(() => cb(null));
        }
        if (this.findNextSeqWaiters) {
            this.findNextSeqWaiters.push(cb);
            return undefined;
        }
        const callWaiters = err => {
            const waiterList = this.findNextSeqWaiters;
            delete this.findNextSeqWaiters;
            waiterList.forEach(cb => cb(err));
        };
        this.findNextSeqWaiters = [cb];
        let lastSeq;
        return openLog.logDb.createKeyStream({ reverse: true, limit: 1 })
            .on('data', key => {
                lastSeq = Number.parseInt(key, 10);
            })
            .on('end', () => {
                if (lastSeq !== undefined) {
                    // eslint-disable-next-line no-param-reassign
                    openLog.nextSeq = lastSeq + 1;
                } else {
                    // eslint-disable-next-line no-param-reassign
                    openLog.nextSeq = 1;
                }
                callWaiters(null);
            })
            .on('error', callWaiters);
    }
}

module.exports = {
    openLog,
    RecordLogService,
};

'use strict'; // eslint-disable-line

const stream = require('stream');

const errors = require('../../../errors');
const jsutil = require('../../../jsutil');

class ListRecordStream extends stream.Transform {
    constructor(logger) {
        super({ objectMode: true });
        this.logger = logger;
    }

    _transform(itemObj, encoding, callback) {
        itemObj.entries.forEach(entry => {
            // eslint-disable-next-line no-param-reassign
            entry.type = entry.type || 'put';
        });
        this.logger.debug('processing log item', { itemObj });
        this.push(itemObj);
        callback();
    }
}


/**
 * @class
 * @classdesc Proxy object to access raft log API
 */
class RecordLogProxy {

    /**
     * @constructor
     *
     * @param {Object} params - constructor params
     * @param {RaftClient} params.bucketClient - client object to bucketd
     * @param {Number} params.raftSession - raft session ID to query
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        this.bucketClient = params.bucketClient;
        this.raftSession = params.raftSession;
        this.logger = params.logger;
    }

    /**
     * Prune the oldest records in the record log
     *
     * Note: not implemented yet
     *
     * @param {Object} params - params object
     * @param {Function} cb - callback when done
     * @return {undefined}
     */
    pruneRecords(params, cb) {
        setImmediate(() => cb(errors.NotImplemented));
    }

    /**
     * Read a series of log records from raft
     *
     * @param {Object} [params] - params object
     * @param {Number} [params.startSeq] - fetch starting from this
     *   sequence number
     * @param {Number} [params.limit] - maximum number of log records
     *   to return
     * @param {function} cb - callback function, called with an error
     *   object or null and an object as 2nd parameter
     * @return {undefined}
     */
    readRecords(params, cb) {
        const recordStream = new ListRecordStream(this.logger);
        const cbOnce = jsutil.once(cb);
        const _params = params || {};

        this.bucketClient.getRaftLog(
            this.raftSession, _params.startSeq, _params.limit,
            false, null, (err, data) => {
                if (err) {
                    if (err.code === 404) {
                        // no such raft session, log and ignore
                        this.logger.warn(
                            'This raft session does not exist (yet)',
                            { raftId: this.raftSession });
                        return cbOnce(null, { info: { start: null,
                                                      end: null } });
                    }
                    if (err.code === 416) {
                        // requested range not satisfiable
                        return cbOnce(null, { info: { start: null,
                                                      end: null } });
                    }
                    this.logger.error(
                        'Error handling record log request', { error: err });
                    return cbOnce(err);
                }
                const logResponse = JSON.parse(data);
                logResponse.log.forEach(entry => recordStream.write(entry));
                recordStream.end();
                return cbOnce(null, { info: logResponse.info,
                                      log: recordStream });
            }, this.logger.newRequestLogger());
    }
}

module.exports = {
    RecordLogProxy,
};

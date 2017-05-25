'use strict'; // eslint-disable-line

const http = require('http');
const stream = require('stream');

const errors = require('../../../errors');

class ListRecordStream extends stream.Transform {
    constructor(logger) {
        super({ objectMode: true });
        this.logger = logger;
        this.buffers = [];
    }

    _transform(data, encoding, callback) {
        this.buffers.push(data);
        callback();
    }

    _flush(callback) {
        const jsonContents = this.buffers.join('');
        const logArray = JSON.parse(jsonContents);
        logArray.forEach(item => {
            this.logger.info('processing log item', { item });
            this.push(item);
        });
        this.push(null);
        callback();
    }
}


/**
 * @class
 * @classdesc Proxy object to access raft log API
 */
class RecordLogProxy {

    constructor(params) {
        this.url = params.url;
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

    readRecords(params) {
        const recordStream = new ListRecordStream(this.logger);
        const req = http.request(this.url);
        req.on('response', response => {
            response.on('error', err => {
                this.logger.error(
                    'Error fetching record log from metadata',
                    { error: err });
                recordStream.emit('error', err);
            });
            response.pipe(recordStream);
        });
        req.on('error', err => {
            this.logger.error(
                'Error sending record log request', { error: err });
            recordStream.emit('error', err);
        });
        req.end();
        return recordStream;
    }
}


/**
 * Open a raft record log, return a proxy object to the raft log API
 *
 * @param {Object} params - openLog params
 * @param {String} params.url - HTTP URL to the record log service
 * @param {Logger} params.logger - logger object
 * @param {Function} done - callback expecting an error argument, or
 *   null and the opened log proxy object on success
 */
function openLog(params, done) {
    const logProxy = new RecordLogProxy({
        url: params.url,
        logger: params.logger
    });
    setImmediate(() => done(null, logProxy));
}


module.exports = {
    openLog
};

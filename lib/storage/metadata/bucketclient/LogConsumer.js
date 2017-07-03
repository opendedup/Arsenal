'use strict'; // eslint-disable-line

const Logger = require('werelogs').Logger;

const { RecordLogProxy } = require('./RecordLogProxy');

class LogConsumer {

    /**
     * @constructor
     *
     * @param {Object} params - constructor params
     * @param {bucketclient.RESTClient} params.bucketClient -
     *   bucketclient instance
     * @param {Logger} [params.log] - logging options
     */
    constructor(params) {
        this.setupLogging(params.log);
        this.bucketClient = params.bucketClient;
    }

    setupLogging(config) {
        let options = undefined;
        if (config !== undefined) {
            options = {
                level: config.logLevel,
                dump: config.dumpLevel,
            };
        }
        this.logger = new Logger('LogConsumer', options);
    }

    /**
     * Open a raft record log, return a proxy object to the raft log API
     *
     * @param {Number} raftSession - raft session number
     * @return {RecordLogProxy} the log proxy object
     */
    openRecordLog(raftSession) {
        return new RecordLogProxy({
            bucketClient: this.bucketClient,
            raftSession,
            logger: this.logger,
        });
    }
}

module.exports = LogConsumer;

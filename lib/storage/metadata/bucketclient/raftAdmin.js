
const { RecordLogProxy } = require('./RecordLog');

/**
 * Open a raft record log, return a proxy object to the raft log API
 *
 * @param {Object} params - openLog params
 * @param {String} params.url - HTTP URL to the record log service
 * @param {Logger} params.logger - logger object
 * @param {Function} done - callback expecting an error argument, or
 *   null and the opened log proxy object on success
 * @return {RecordLogProxy} the log proxy object
 */
function openRecordLog(params, done) {
    const logProxy = new RecordLogProxy({
        url: params.url,
        logger: params.logger,
    });
    return logProxy;
}


module.exports = {
    openRecordLog,
};

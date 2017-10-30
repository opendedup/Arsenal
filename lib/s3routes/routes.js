const assert = require('assert');

const errors = require('../errors');
const routeGET = require('./routes/routeGET');
const routePUT = require('./routes/routePUT');
const routeDELETE = require('./routes/routeDELETE');
const routeHEAD = require('./routes/routeHEAD');
const routePOST = require('./routes/routePOST');
const routeOPTIONS = require('./routes/routeOPTIONS');
const routesUtils = require('./routesUtils');
const routeWebsite = require('./routes/routeWebsite');

const routeMap = {
    GET: routeGET,
    PUT: routePUT,
    POST: routePOST,
    DELETE: routeDELETE,
    HEAD: routeHEAD,
    OPTIONS: routeOPTIONS,
};

function checkUnsupportedRoutes(reqMethod, reqQuery, unsupportedQueries, log) {
    const method = routeMap[reqMethod];
    if (!method) {
        return { error: errors.MethodNotAllowed };
    }

    if (routesUtils.isUnsupportedQuery(reqQuery, unsupportedQueries)) {
        log.debug('encountered unsupported query');
        return { error: errors.NotImplemented };
    }

    return { method };
}

function checkBucketAndKey(bucketName, objectKey, method, reqQuery,
    blacklistedPrefixes, log) {
    // if empty name and request not a list Buckets
    if (!bucketName && !(method === 'GET' && !objectKey)) {
        log.debug('empty bucket name', { method: 'routes' });
        return (method !== 'OPTIONS') ?
            errors.MethodNotAllowed : errors.AccessForbidden
               .customizeDescription('CORSResponse: Bucket not found');
    }
    if (bucketName !== undefined && routesUtils.isValidBucketName(bucketName,
        blacklistedPrefixes.bucket) === false) {
        log.debug('invalid bucket name', { bucketName });
        return errors.InvalidBucketName;
    }
    if ((reqQuery.partNumber || reqQuery.uploadId)
        && objectKey === undefined) {
        return errors.InvalidRequest
            .customizeDescription('A key must be specified');
    }
    return undefined;
}

function checkTypes(req, res, params, logger) {
    assert.strictEqual(typeof req, 'object',
        'bad routes param: req must be an object');
    assert.strictEqual(typeof res, 'object',
        'bad routes param: res must be an object');
    assert.strictEqual(typeof logger, 'object',
        'bad routes param: logger must be an object');
    assert.strictEqual(typeof params.api, 'object',
        'bad routes param: api must be an object');
    assert.strictEqual(typeof params.api.callApiMethod, 'function',
        'bad routes param: api.callApiMethod must be a defined function');
    assert.strictEqual(typeof params.healthcheckHandler, 'function',
        'bad routes param: healthcheckHandler must be a function');
    if (params.statsClient) {
        assert.strictEqual(typeof params.statsClient, 'object',
        'bad routes param: statsClient must be an object');
    }
    assert(Array.isArray(params.allEndpoints),
        'bad routes param: allEndpoints must be an array');
    assert(params.allEndpoints.length > 0,
        'bad routes param: allEndpoints must have at least one endpoint');
    params.allEndpoints.forEach(endpoint => {
        assert.strictEqual(typeof endpoint, 'string',
        'bad routes param: each item in allEndpoints must be a string');
    });
    assert(Array.isArray(params.websiteEndpoints),
        'bad routes param: allEndpoints must be an array');
    params.websiteEndpoints.forEach(endpoint => {
        assert.strictEqual(typeof endpoint, 'string',
        'bad routes param: each item in websiteEndpoints must be a string');
    });
    assert.strictEqual(typeof params.blacklistedPrefixes, 'object',
        'bad routes param: blacklistedPrefixes must be an object');
    assert(Array.isArray(params.blacklistedPrefixes.bucket),
        'bad routes param: blacklistedPrefixes.bucket must be an array');
    params.blacklistedPrefixes.bucket.forEach(pre => {
        assert.strictEqual(typeof pre, 'string',
        'bad routes param: each blacklisted bucket prefix must be a string');
    });
    assert(Array.isArray(params.blacklistedPrefixes.object),
        'bad routes param: blacklistedPrefixes.object must be an array');
    params.blacklistedPrefixes.object.forEach(pre => {
        assert.strictEqual(typeof pre, 'string',
        'bad routes param: each blacklisted object prefix must be a string');
    });
    assert.strictEqual(typeof params.unsupportedQueries, 'object',
        'bad routes param: unsupportedQueries must be an object');
    assert.strictEqual(typeof params.dataRetrievalFn, 'function',
        'bad routes param: dataRetrievalFn must be a defined function');
}

/** routes - route request to appropriate method
 * @param {Http.Request} req - http request object
 * @param {Http.ServerResponse} res - http response sent to the client
 * @param {object} params - additional routing parameters
 * @param {object} params.api - all api methods and method to call an api method
 *  i.e. api.callApiMethod(methodName, request, response, log, callback)
 * @param {function} params.healthcheckHandler - healthcheck function
 *  parameters: (clientIP, deep, req, res, log, statsClient)
 * @param {StatsClient} [params.statsClient] - client to report stats to Redis
 * @param {string[]} params.allEndpoints - all accepted REST endpoints
 * @param {string[]} params.websiteEndpoints - all accepted website endpoints
 * @param {object} params.blacklistedPrefixes - blacklisted prefixes
 * @param {string[]} params.blacklistedPrefixes.bucket - bucket prefixes
 * @param {string[]} params.blacklistedPrefixes.object - object prefixes
 * @param {object} params.unsupportedQueries - object containing true/false
 *  values for whether queries are supported
 * @param {function} params.dataRetrievalFn - function to retrieve data
 * @param {RequestLogger} logger - werelogs logger instance
 * @returns {undefined}
 */
function routes(req, res, params, logger) {
    checkTypes(req, res, params, logger);

    const {
        api,
        healthcheckHandler,
        statsClient,
        allEndpoints,
        websiteEndpoints,
        blacklistedPrefixes,
        unsupportedQueries,
        dataRetrievalFn,
    } = params;

    const clientInfo = {
        clientIP: req.socket.remoteAddress,
        clientPort: req.socket.remotePort,
        httpMethod: req.method,
        httpURL: req.url,
        endpoint: req.endpoint,
    };

    const log = logger.newRequestLogger();
    log.debug('received request', clientInfo);

    log.end().addDefaultFields(clientInfo);

    if (req.url === '/_/healthcheck') {
        return healthcheckHandler(clientInfo.clientIP, false, req, res, log,
            statsClient);
    } else if (req.url === '/_/healthcheck/deep') {
        return healthcheckHandler(clientInfo.clientIP, true, req, res, log);
    }
    if (statsClient) {
        // report new request for stats
        statsClient.reportNewRequest();
    }

    try {
        const validHosts = allEndpoints.concat(websiteEndpoints);
        routesUtils.normalizeRequest(req, validHosts);
    } catch (err) {
        log.debug('could not normalize request', { error: err.stack });
        return routesUtils.responseXMLBody(
            errors.InvalidURI.customizeDescription('Could not parse the ' +
                'specified URI. Check your restEndpoints configuration.'),
                undefined, res, log);
    }

    log.addDefaultFields({
        bucketName: req.bucketName,
        objectKey: req.objectKey,
        bytesReceived: req.parsedContentLength || 0,
        bodyLength: parseInt(req.headers['content-length'], 10) || 0,
    });

    const reqMethod = req.method.toUpperCase();

    const { error, method } = checkUnsupportedRoutes(reqMethod, req.query,
        unsupportedQueries, log);

    if (error) {
        log.trace('error validating route or uri params', { error });
        return routesUtils.responseXMLBody(error, null, res, log);
    }

    const bucketOrKeyError = checkBucketAndKey(req.bucketName, req.objectKey,
        reqMethod, req.query, blacklistedPrefixes, log);

    if (bucketOrKeyError) {
        log.trace('error with bucket or key value',
        { error: bucketOrKeyError });
        return routesUtils.responseXMLBody(bucketOrKeyError, null, res, log);
    }

    // bucket website request
    if (websiteEndpoints && websiteEndpoints.indexOf(req.parsedHost) > -1) {
        return routeWebsite(req, res, api, log, statsClient, dataRetrievalFn);
    }

    return method(req, res, api, log, statsClient, dataRetrievalFn);
}

module.exports = routes;

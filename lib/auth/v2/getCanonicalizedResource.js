'use strict'; // eslint-disable-line strict

const url = require('url');

/*
If request includes a specified subresource,
add to the resourceString: (a) a '?', (b) the subresource,
and (c) its value (if any).
Separate multiple subresources with '&'.
Subresources must be in alphabetical order.
*/

// Specified subresources:
const subresources = [
    'acl',
    'lifecycle',
    'location',
    'logging',
    'notification',
    'partNumber',
    'policy',
    'requestPayment',
    'torrent',
    'uploadId',
    'uploads',
    'versionId',
    'versioning',
    'versions',
    'website',
];

// Specified subresources:
const subresourcesMap = {
    'acl': true,
    'lifecycle': true,
    'location': true,
    'logging': true,
    'notification': true,
    'partNumber': true,
    'policy': true,
    'requestPayment': true,
    'torrent': true,
    'uploadId': true,
    'uploads': true,
    'versionId': true,
    'versioning': true,
    'versions': true,
    'website': true,
};

/*
If the request includes parameters in the query string,
that override the headers, include
them in the resourceString
along with their values.
AWS is ambiguous about format.  Used alphabetical order.
*/
const overridingParams = [
    'response-cache-control',
    'response-content-disposition',
    'response-content-encoding',
    'response-content-language',
    'response-content-type',
    'response-expires',
];

function makeValue(query, key) {
    const val = query[key];
    if (val !== '') {
        return key + '=' + val;
    }
    return key;
}

function getCanonicalizedResource(request) {
    /*
    This variable is used to determine whether to insert
    a '?' or '&'.  Once a query parameter is added to the resourceString,
    it changes to '&' before any new query parameter is added.
    */
    let queryChar = '?';
    // If bucket specified in hostname, add to resourceString
    let resourceString = request.gotBucketNameFromHost ?
        `/${request.bucketName}` : '';
    // Add the path to the resourceString
    const url = request.url;
    let index = url.indexOf('?');
    let pathname = url;
    if (index === -1) {
        index = url.indexOf('#');
    }
    if (index !== -1) {
        pathname = url.substring(0, index);
    }

    resourceString += pathname; 

    // Check which specified subresources are present in query string,
    // build array with them
    const query = request.query;
    const presentSubresources = [];
    const queryKeys = Object.keys(query);
    const queryKeysLen = queryKeys.length;
    for (let i = 0; i < queryKeysLen; ++i) {
        const key = queryKeys[i];
        if (subresourcesMap[key]) {
            // Sort the array and add the subresources and their value (if any)
            // to the resourceString
            const subResourcesLen = presentSubresources.length;
            if (subResourcesLen === 0) {
                presentSubresources.push(makeValue(query, key));
                continue;
            }
            for (let j = 0; j < subResourcesLen; ++j) {
                if (key < presentSubresources[j]) {
                    presentSubresources.splice(j, 0, makeValue(query, key));
                    break;
                }
                if (j === subResourcesLen - 1) {
                    presentSubresources.push(makeValue(query, key));
                }
            }
        }
    }
    // const presentSubresources = Object.keys(query).filter(val =>
    //     subresources.indexOf(val) !== -1);
    // presentSubresources.sort();
    const subResourcesLen = presentSubresources.length;
    if (subResourcesLen > 0) {
        queryChar = '&';
        resourceString += '?' + presentSubresources.join('&');
    }
    // resourceString = presentSubresources.reduce((prev, current) => {
    //     const ch = (query[current] !== '' ? '=' : '');
    //     const ret = `${prev}${queryChar}${current}${ch}${query[current]}`;
    //     queryChar = '&';
    //     return ret;
    // }, resourceString);
    // Add the overriding parameters to our resourceString
    const overridingParamsLen = overridingParams.length;
    for (let i = 0; i < overridingParamsLen; ++i) {
        const current = overridingParams[i];
        const value = query[current];
        if (value) {
            resourceString += `${queryChar}${current}=${value}`;
            queryChar = '&';
        }
    }

    /*
    Per AWS, the delete query string parameter must be included when
    you create the CanonicalizedResource for a multi-object Delete request.
    Unclear what this means for a single item delete request.
    */
    if (request.query.delete) {
        // Addresses adding '?' instead of '&' if no other params added.
        resourceString += `${queryChar}delete=${query.delete}`;
    }
    return resourceString;
}

module.exports = getCanonicalizedResource;
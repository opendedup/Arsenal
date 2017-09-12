/**
 * Helper class to ease access to a single data location in metadata
 * 'location' array
 */
class ObjectMDLocation {

    constructor(locationObj) {
        this._data = {
            key: locationObj.key,
            start: locationObj.start,
            size: locationObj.size,
            dataStoreName: locationObj.dataStoreName,
            dataStoreETag: locationObj.dataStoreETag,
        };
    }

    getKey() {
        return this._data.key;
    }

    getDataStoreName() {
        return this._data.dataStoreName;
    }

    setDataLocation(location) {
        this._data.key = location.key;
        this._data.dataStoreName = location.dataStoreName;
    }

    getDataStoreETag() {
        return this._data.dataStoreETag;
    }

    getPartNumber() {
        return Number.parseInt(this._data.dataStoreETag.split(':')[0], 10);
    }

    getPartETag() {
        return this._data.dataStoreETag.split(':')[1];
    }

    getPartStart() {
        return this._data.start;
    }

    getPartSize() {
        return this._data.size;
    }

    setPartSize(size) {
        this._data.size = size;
    }

    getValue() {
        return this._data;
    }
}

module.exports = ObjectMDLocation;

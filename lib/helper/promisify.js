const util = require('util');

function promisify(redisClient, fn) {
    return util.promisify(fn).bind(redisClient);
}

module.exports = function (redisClient) {
    return Object.freeze({
        get: promisify(redisClient, redisClient.get),
        setnx: promisify(redisClient, redisClient.setnx),
        getset: promisify(redisClient, redisClient.getset),
    });
};

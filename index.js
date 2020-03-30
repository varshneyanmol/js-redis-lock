module.exports = function (redisClient) {
    const LockingMechanism = require('./lib/LockingMechanism');
    return new LockingMechanism(redisClient);
};
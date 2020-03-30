class LockingMechanism {

    // pass-in the redis-client object
    constructor(redisClient) {
        this.promisifiedRedisFunctions = require('./helper/promisify')(redisClient);
        this.redis = redisClient;
    }

    _makeKey(key) {
        return "lock-" + key;
    }

    _getTimeOut(ttl) {
        return new Date().getTime() + ttl;
    }

    lock(key, ttl = 300) {
        return new Promise(async (resolve, reject) => {
            if (typeof key !== 'string') {
                return reject({name: "key error", message: "key must be string"});
            }

            //tryToLock will be 1/0. if 0, lock is already acquired by some other process
            const lockKey = this._makeKey(key);
            let lockTimeOut = this._getTimeOut(ttl);

            const tryToLock = await this.promisifiedRedisFunctions.setnx(lockKey, lockTimeOut);

            if (tryToLock === 1) {
                this.redis.watch(lockKey);
                return resolve(1);
            }

            // get() returns value stored at key OR null.
            const currentLockTimestamp = await this.promisifiedRedisFunctions.get(lockKey);

            // if currentLockTimestamp = null, "if" statement below will not be executed
            if (currentLockTimestamp && currentLockTimestamp >= new Date().getTime()) {
                // lock is already acquired by some other process
                return resolve(0);
            }

            // lock was acquired by some other process, but it has been timed out because of:
            // 1. either process too long than its estimated time or,
            // 2. process crashed
            // so, overwrite this lock with current value
            lockTimeOut = this._getTimeOut(ttl);
            const prevLockTimeStamp = await this.promisifiedRedisFunctions.getset(lockKey, lockTimeOut);
            if (prevLockTimeStamp && prevLockTimeStamp >= new Date().getTime()) {
                // some other process acquired the lock before it's getset
                return resolve(0);
            }

            // lock acquired successfully
            this.redis.watch(lockKey);
            return resolve(1);
        });
    }

    _sleep(milliseconds) {
        return new Promise((resolve, reject) => setTimeout(resolve, milliseconds));
    }

    retryableLock(key, ttl = 500, retryAfter = 150, maxAttempts = 0) {
        return new Promise(async (resolve, reject) => {
            const keepRetrying = maxAttempts < 1;
            let attempts = 0;

            while (keepRetrying || (attempts < maxAttempts)) {
                let result;
                try {
                    result = await this.lock(key, ttl);
                } catch (err) {
                    return reject(err);
                }

                if (result === 1) {
                    return resolve(1);
                }
                attempts++;
                await this._sleep(retryAfter);
            }
            resolve(0);
        });
    }

    unlock(key) {
        return new Promise((resolve, reject) => {
            if (typeof key !== 'string') {
                return reject({name: "key error", message: "key must be string"});
            }

            const lockKey = this._makeKey(key);

            this.redis
                .multi()
                .del(lockKey)
                .exec((err, result) => {
                    if (err) {
                        return reject(err);
                    }

                    if (result === null) {
                        // this means transaction was aborted because some other process modified the lock-key value.
                        // It means some other process has the lock currently.
                        return resolve(0);
                    }
                    // "result" will be an array as it is result of exec command which executes all the command in the transaction
                    //  and returns the result of all those commands in an array.
                    // Since we have only "del" command in this transaction, result array size will be 1.
                    // Value at result[0] will be the number of keys deleted as a result of del command.
                    return resolve(result[0]);
                });
        });
    }

    readWithLock(key) {
        return new Promise((resolve, reject) => {
            if (typeof key !== 'string') {
                return reject({name: "key error", message: "key must be string"});
            }

            const lockKey = this._makeKey(key);

            // here I'm reading key's value with its lock key in a transaction.
            // This makes sure that no key modifies the value in between.
            // So, if lock-key exists AND it has not yet expired, then I can NOT trust the key's value returned because some other process is inside critical section with the write lock.
            // Rest in all cases, key's value is guaranteed to be consistent.
            this.redis
                .multi()
                .get(lockKey)
                .get(key)
                .exec((err, result) => {
                    if (err) {
                        return reject(err);
                    }

                    if (result === null) {
                        // this means transaction was aborted because some other process modified the lock-key value.
                        // It means some other process has the lock currently.
                        return reject(0);
                    }
                    // "result" will be an array as it is result of exec command which executes all the command in the transaction
                    //  and returns the result of all those commands in an array.
                    // Value at result[0] will be the result of get(lockKey) command.
                    // Value at result[1] will be the result of get(key command).

                    const [currentLockTimestamp, value] = result;
                    if (currentLockTimestamp && currentLockTimestamp >= new Date().getTime()) {
                        return reject(0);
                    }

                    return resolve(value);
                });
        });
    }

    readWithRetryableLock(key, retryAfter = 150, maxAttempts = 0) {
        return new Promise(async (resolve, reject) => {
            const keepRetrying = maxAttempts < 1;
            let attempts = 0;

            while (keepRetrying || (attempts < maxAttempts)) {
                let result;
                try {
                    result = await this.readWithLock(key);
                } catch (err) {
                    // if err === 0, it means that read lock failed
                    if (err !== 0) {
                        return reject(err);
                    } else {
                        attempts++;
                        await this._sleep(retryAfter);
                        continue;
                    }
                }
                return resolve(result);
            }
            return reject(0);
        });
    }

}

module.exports = LockingMechanism;

const Engine = require('npc-engine');
const nodePersist = require('node-persist');
const Bottleneck = require('bottleneck');
const defaultsDeep = require('lodash.defaultsdeep');
const path = require('path');

class NodePersist extends Engine {
	#engine;
	#limiter;
	#directory;
	#ready;
	#defaultOpts = {
		directory: `.npc/storage`
	};

	// Prevent more than one node-persist from using the same directory
	static #cacheDirectories = {};

	/**
	 * Attempt to acquire a lock on the given directory
	 * If another NodePersist has already locked the directory, waits
	 *   up to 500 ms for the other engine to release, then rejects
	 *
	 * @async
	 * @param {string} directory - The directory to lock
	 * @return {Promise} - Promise representing the lock, or rejection if already locked
	 */
	static #lockDirectory(directory) {
		return new Promise((resolve, reject) => {
			if (NodePersist.#cacheDirectories[directory]) {
				// If this directory is locked, another NodePersist instance
				//   is using it, but it might be shutting down right now
				// Give it 500 ms and then reject
				setTimeout(() => {
					reject(new Error(`Another NodePersist engine has already locked directory '${directory}'; use prefix to allow multiple node-persist instances`));
				}, 500);

				return resolve(NodePersist.#cacheDirectories[directory].lock);
			} else {
				// If this directory is not locked, just proceed
				return resolve();
			}
		}).then(() => {
			let unlock;

			const lock = new Promise((resolve, reject) => {
				unlock = resolve;
			});

			NodePersist.#cacheDirectories[directory] = {
				lock,
				unlock
			};

			return lock;
		});
	}

	/**
	 * Unlock the given directory, if it is locked
	 *
	 * @param {string} directory - The directory to unlock
	 * @return {boolean} - Always returns true
	 */
	static #unlockDirectory(directory) {
		if (NodePersist.#cacheDirectories[directory]) {
			NodePersist.#cacheDirectories[directory].unlock();
			delete NodePersist.#cacheDirectories[directory];
		}
		return true;
	}

	constructor(opts, cache) {
		super(opts, cache);

		defaultsDeep(opts, this.#defaultOpts);

		// This will allow node-persist to provide multiple distinct caches
		//   without collisions
		if (opts.prefix) {
			opts.directory = path.join(opts.directory, opts.prefix);
		}

		let readyResolve
		,	readyReject;
		this.#ready = new Promise((resolve, reject) => {
			readyResolve = resolve;
			readyReject = reject;
		});

		NodePersist.#lockDirectory(opts.directory).then(() => {
			this.#directory = opts.directory;
			this.#engine = nodePersist.create({
				dir: this.#directory
			});
			this.#limiter = new Bottleneck({
				maxConcurrent: 1
			});

			readyResolve();
		}).catch(readyReject);
	}

	/**
	 * Define how a single stored datum should be returned when loaded
	 *
	 * @typedef {Object} datum
	 * @property {string} key - The key of the entry
	 * @property {*} val - The stored value of the entry
	 * @property {(string|undefined)} ttl - The TTL of the entry, in milliseconds
	 */

	/**
	 * Get a value - note that this is unlikely to be used
	 *
	 * @async
	 * @param {string} key - The key to get
	 * @return {*} - The value stored in persistent data, or undefined
	 */
	async get(key) {
		await this.#ready;
		return this.#limiter.schedule(this.#engine.getItem.bind(this.#engine), key);
	}

	/**
	 * Set a value
	 *
	 * @async
	 * @param {string} key - The key to set
	 * @param {*} val - The value to store
	 * @param {number} [ttl] - The TTL, in milliseconds
	 * @return {*} - The value stored
	 */
	async set(key, val, ttl) {
		await this.#ready;
		if (ttl === undefined) {
			ttl = this.cache.getTtl(key);
		}
		await this.#limiter.schedule(this.#engine.updateItem.bind(this.#engine), key, val, { ttl });

		return val;
	}

	/**
	 * Delete a value
	 *
	 * @async
	 * @param {string} key - The key to delete
	 * @return {undefined}
	 */
	async del(key) {
		await this.#ready;
		return this.#limiter.schedule(this.#engine.removeItem.bind(this.#engine), key);
	}

	/**
	 * Update a value's TTL
	 *
	 * @async
	 * @param {string} key - The key to update
	 * @param {number} ttl - The TTL, in milliseconds
	 * @return {(number|undefined)}
	 */
	async ttl(key, ttl) {
		await this.#ready;
		// node-persist does not expose a way to set a ttl,
		//   so we have to re-retrieve the value and re-set the datum
		let val = await this.cache.get(key);
		let result = undefined;
		if (val !== undefined) {
			await this.set(key, val, ttl);
		}
	}

	/**
	 * Load all previously-persisted records
	 *
	 * @typedef {Object} datum
	 * @property {string} key - The key of the entry
	 * @property {*} val - The stored value of the entry
	 * @property {(number|undefined)} ttl - The TTL of the entry, in milliseconds
	 *
	 * @async
	 * @return {datum[]}
	 */
	async load() {
		await this.#ready;
		await this.#engine.init();

		let result = [ ];

		await this.#engine.forEach((datum) => {
			// Engine.forEach returns all data without inspecting ttl
			// That means it may return expired data!
			// So check that TTL
			if (!datum.ttl || Date.now() < datum.ttl) {
				// Although both node-persist and node-cache store ms timestamps,
				//   node-cache *sets* TTLs in seconds
				// node-persist sets TTLs in milliseconds
				let ttl = this.tsToTtl(datum.ttl);
				if (ttl) {
					ttl /= 1000;
				}
				result.push({key: datum.key, val: datum.value, ttl });
			}
		});

		return result;
	}

	/**
	 * Clear all records
	 *
	 * @async
	 * @return {boolean} - Always returns true
	 */
	async flush() {
		await this.#ready;
		await this.#limiter.schedule(this.#engine.clear.bind(this.#engine));
		return true;
	}

	/**
	 * Release directory - DO NOT clear records!
	 *
	 * @async
	 * @return {boolean} - Always returns true
	 */
	close() {
		// Release the directory 
		//delete cacheDirectories[this.#directory];
		NodePersist.#unlockDirectory(this.#directory);
		return true;
	}
};

module.exports = NodePersist;
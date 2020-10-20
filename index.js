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
	 * @return {Promise} - Promise representing success, or rejection if already locked
	 */
	static #lockDirectory(directory, forEngine) {
		forEngine.cache.log.debug(`engine.${forEngine.id}|lockDirectory ${directory}`);

		return new Promise((resolve, reject) => {
			if (NodePersist.#cacheDirectories[directory]) {
				forEngine.cache.log.debug(`engine.${forEngine.id}|lockDirectory ${directory} is being used by another engine`);

				// If this directory is locked, another NodePersist instance
				//   is using it, but it might be shutting down right now
				// Give it 500 ms and then reject
				setTimeout(() => {
					forEngine.cache.log.debug(`engine.${forEngine.id}|lockDirectory ${directory} lock acquisition timeout exceeded - rejecting`);
					reject(new Error(`Another NodePersist engine has already locked directory '${directory}'; use prefix to allow multiple node-persist instances`));
				}, 500);

				return resolve(NodePersist.#cacheDirectories[directory].lock);
			} else {
				forEngine.cache.log.debug(`engine.${forEngine.id}|lockDirectory ${directory} is available`);

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

			forEngine.cache.log.debug(`engine.${forEngine.id}|lockDirectory ${directory} lock acquired`);

			return true;
		});
	}

	/**
	 * Unlock the given directory, if it is locked
	 *
	 * @param {string} directory - The directory to unlock
	 * @return {boolean} - Always returns true
	 */
	static #unlockDirectory(directory, forEngine) {
		forEngine.cache.log.debug(`engine.${forEngine.id}|unlockDirectory ${directory}`);

		if (NodePersist.#cacheDirectories[directory]) {
			forEngine.cache.log.debug(`engine.${forEngine.id}|unlockDirectory ${directory}: calling unlock on engine`);

			NodePersist.#cacheDirectories[directory].unlock();
			delete NodePersist.#cacheDirectories[directory];
		} else {
			forEngine.cache.log.debug(`engine.${forEngine.id}|unlockDirectory ${directory}: no lock present`);
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
		
		this.#directory = opts.directory;
		this.cache.log.debug(`engine.${this.id}|initializing in directory ${this.#directory}`);

		let readyResolve
		,	readyReject;
		this.#ready = new Promise((resolve, reject) => {
			readyResolve = resolve;
			readyReject = reject;
		});

		NodePersist.#lockDirectory(this.#directory, this).then(() => {
			this.#engine = nodePersist.create({
				dir: this.#directory
			});
			this.#limiter = new Bottleneck({
				maxConcurrent: 1
			});

			this.cache.log.debug(`engine.${this.id}|initialized in directory ${this.#directory}`);

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

		this.cache.log.debug(`engine.${this.id}|get ${key}`);

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

		this.cache.log.debug(`engine.${this.id}|set ${key} (TTL: ${ttl}):`, val);

		if (ttl === undefined) {
			ttl = this.cache.getTtl(key);
			if (ttl) {
				ttl = ttl - Date.now();
				if (ttl < 0) { // TTL was actually in the past - instant delete
					await this.del(key);
					return undefined;
				}
			}
		}

		this.cache.log.debug(`engine.${this.id}|set ${key} TTL:`, ttl);

		await this.#limiter.schedule(this.#engine.updateItem.bind(this.#engine), key, val, { ttl });

		this.cache.log.debug(`engine.${this.id}|set ${key} complete`);

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

		this.cache.log.debug(`engine.${this.id}|del ${key}`);

		let result = await this.#limiter.schedule(this.#engine.removeItem.bind(this.#engine), key);

		this.cache.log.debug(`engine.${this.id}|del ${key} complete`);

		return result;
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

		this.cache.log.debug(`engine.${this.id}|ttl ${key} (TTL: ${ttl})`);

		// node-persist does not expose a way to set a ttl,
		//   so we have to re-retrieve the value and re-set the datum
		let val = await this.cache.get(key)
		,	result = undefined;
		if (val !== undefined) {
			this.cache.log.debug(`engine.${this.id}|ttl ${key}: updating value`);

			result = await this.set(key, val, ttl);
		} else {
			this.cache.log.debug(`engine.${this.id}|ttl ${key}: does not exist`);
		}

		return result;
	}

	/**
	 * Load all previously-persisted records
	 *
	 * @async
	 * @return {datum[]}
	 */
	async load() {
		await this.#ready;

		this.cache.log.debug(`engine.${this.id}|init`);

		await this.#engine.init();

		this.cache.log.debug(`engine.${this.id}|init complete`);

		let result = [ ];

		await this.#engine.forEach((datum) => {
			// Engine.forEach returns all data without inspecting ttl
			// That means it may return expired data!
			// So check that TTL
			if (!datum.ttl || Date.now() < datum.ttl) {
				result.push({ key: datum.key, val: datum.value, ttl: this.tsToTtl(datum.ttl) });
			}
		});

		this.cache.log.debug(`engine.${this.id}|load complete:`, result);

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

		this.cache.log.debug(`engine.${this.id}|flush`);

		await this.#limiter.schedule(this.#engine.clear.bind(this.#engine));

		this.cache.log.debug(`engine.${this.id}|flush complete`);

		return true;
	}

	/**
	 * Release directory - DO NOT clear records!
	 *
	 * @async
	 * @return {boolean} - Always returns true
	 */
	async close() {
		await this.#ready;

		this.cache.log.debug(`engine.${this.id}|close`);

		await this.#limiter.stop({ dropWaitingJobs: false });

		this.cache.log.debug(`engine.${this.id}|closed job scheduler`);

		// Release the directory
		NodePersist.#unlockDirectory(this.#directory, this);

		this.cache.log.debug(`engine.${this.id}|released directory ${this.#directory}`);

		return true;
	}
};

module.exports = NodePersist;
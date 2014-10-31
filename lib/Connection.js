!function(){

	var   Class 		= require('ee-class')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, arg 			= require('ee-arguments')
		, async 		= require('ee-async')
		, argv 			= require('ee-argv')
		, log 			= require('ee-log');


	var   dev 		= argv.has('dev-db')
		, toString 	= Object.prototype.toString;


	module.exports = new Class({
		inherits: EventEmitter

		// idle status
		, _idle: true

		// flag if the connection has already ended
		, _ended: false

		// if set to true, dont return the connection to the pool
		, _dontPool: false

		// counts how many queries where executed
		, _numQueries: 0

		// differnet locks that can be obtained by the user on a transaction
        // the orm does its best to get a consistent behavior across the different
        // rdbms systems, which is sadly not possible with locks.
        // see the lock description inside the specific driver
        , LOCK_READ:        {enumerable: true, value: 'LOCK_READ'}
        , LOCK_WRITE:       {enumerable: true, value: 'LOCK_WRITE'}
        , LOCK_EXCLUSIVE:   {enumerable: true, value: 'LOCK_EXCLUSIVE'}


        // valid query modes
        , _validModes: {
        	  query: true
        	, insert: true
        	, update: true
        	, delete: true
        }


		/**
		 * class constructor, must be called from inheriting class
		 *
		 * @param <Object> connection configuration
		 */
		, init: function(options, id) {
			this.options = options;

			this.id = id;

			// check if the interface is correct implemented
			this._checkInterface();
			this._connect(function(err) {
				this.emit('load', err);

				if (err) {
					this._disconnected();
					this._end(err);
				}
				else {
					this._connected();
				}
			}.bind(this));
		}


		/**
		 * the query() method is called for executing sql queries
		 * you may pass arguments in any order. if the arguments contain
		 * a string the query will be handled as raw sql query. the parameters
		 * for a raw sql query may either be passed using an array (the SQL string
		 * should contain question marks) or as object (the sql should contain named
		 * markers like ?name).
		 *
		 * @param <String> SQL query, when in raw SQL mode
		 * @param <Object> parameters for SQL in SQL mode, optional. query when in Objec mode.
		 * @param <Array> optional paremeter when in SQL mode
		 * @param <Boolean> optional, indicates if the query is readonly
		 */
		, query: function() {
			var   l 			= arguments.length
				, i 			= 0
				, results 	 	= []
				, parameters 	= {}
				, mode 			= 'query'
				, readOnly 		= true
				, completed 	= 0
				, queries
				, renderedQueries
				, error
				, callback
				, query
				, data
				, item
				, done
				, i;


			// mark the connection as busy
			this._setBusy();


			// find arguments of the correct type, they can be passed dynamically
			for (;i < l;i++) {
				item = arguments[i];

				switch(toString.call(item).substr(8, 1)) {
					case 'F': // function, always the callback
						callback = arguments[i];
						break;

					case 'S': // string
						if (this._validModes[item])	mode = item;
						else query = item;
						break;

					case 'B': // boolean, always the query modwe
						readOnly = item;
						break;

					case 'A': // array, either a list of queries or a list of argumets to be inserted into an sql query
						queries = item;
						break;

					case 'O': // object, either parameters for an sql query, or an query
						parameters = item;
						break;
				}
			}

			//log('-----------', query, parameters, queries);

			// we need to decide in which mode the data was passed
			// SQL String mode
			if (query) {
				queries = [{SQLString: query, parameters: (parameters || queries)}]
			}
			// must be in object mode
			else if (!queries && parameters) {
				queries = [this._render(mode, parameters)];
			}
			// multiple queries
			else if (queries && queries.length) {
				renderedQueries = [];
				for (i = 0, l = queries.length; i < l; i++) renderedQueries.push(this._render(mode, queries[i]));
			}
			// can't work with this
			else {
				throw new Error('Invalid input for the query method!');
			}


			// callback for the queries
			done = function(err, data) {
				if (err) {
					if (callback) callback(err);
					callback = null;
					this._setIdle();
				}
				else {
					results.push(data);

					if (++completed === queries.length) {
						// we're done
						if (queries.length === 1) callback(null, results[0]);
						else callback(null, results);

						this._setIdle();
					}
				}
			}.bind(this);

			//log(queries);

			// execute the queries
			for (i = 0, l = queries.length; i < l; i++) this._executeOneQuery(mode, queries[i], done);
		}



		
		/*
		 * execute the query
		 */
		, _executeOneQuery: function(mode, query, callback) {
			var SQLString;

			query.SQLString += ';';

			// fill parameterized queries
			try {
				SQLString = this._fillSQL(query.SQLString, query.parameters);
			} catch (e) {
				return callback(new Error('Failed to build SQL string: '+e));
			}
			
			
			// execute the query
			this._query(SQLString, callback);
		}



		, close: function(){
			this._end();
		}


		, removeFromPool: function() {
			if (dev) log.highlight('removed connection from pool ...');
			this._setBusy();
			this._dontPool = true;
		}


		, startTransaction: function(callback) {
			if (dev) log.highlight('started transaction ...');
			this.isTransaction = true;
			this._setBusy();
			this.queryRaw('start transaction;', callback);
		}


		, commit: function(callback) {
			if (dev) log.highlight('commited transaction ...');
			this.queryRaw('commit;', function(err){
				callback(err);
				this._end();
			}.bind(this));
		}


		, rollback: function(callback) {
			if (dev) log.highlight('rolled back transaction ...');
			this.queryRaw('rollback;', function(err){
				callback(err);
				this._end();
			}.bind(this));
		}



		, queryRaw: function() {
			var   SQLString		= arg(arguments, 'string')
				, callback 		= arg(arguments, 'function', function(){})
				, parameters	= arg(arguments, 'array', arg(arguments, 'object', {}))
				, readOnly 		= arg(arguments, 'boolean', true);

			this._setBusy();

			SQLString = this._fillSQL(SQLString, parameters);

			this._query(SQLString, function(err, data) {
				callback(err, data);
				this._setIdle();
			}.bind(this));
		}



		, describe: function(databases, callback){
			this._describe(databases, callback);
		}


		/**
		 * the fillSQL() method safely replaces placeholders by values in sql.
		 *
		 * @param <String> sql string
		 * @param <Object/Array> values to insert into the sql string
		 */
		, _fillSQL: function(SQLString, parameters) {

			// parameters array, placeholder = «?»
			if (type.array(parameters)) return this._fillByArray(SQLString, parameters);

			// parameters object, placeholder = «?key»
			else if(type.object(parameters)) return this._fillByObject(SQLString, parameters);

			// nothing to do here
			else return SQLString;
		}




		/**
		 * the _fillByObject() method safely replaces placeholders by values in sql.
		 *
		 * @param <String> sql string
		 * @param <Object> values to insert into the sql string
		 */
		, _fillByObject: function(SQLString, parameters) {
			var   keys = Object.keys(parameters)
				, l = keys.length;

			while(l--){
				result = parameters[keys[l]] === null ? null : this._escape(this._toString(parameters[keys[l]]));
				SQLString = SQLString.replace(new RegExp('\\?'+keys[l], 'gi'), result);
			}

			return SQLString;
		}



		/**
		 * the _fillByArray() method safely replaces placeholders by values in sql.
		 *
		 * @param <String> sql string
		 * @param <Array> values to insert into the sql string
		 */
		, _fillByArray: function(SQLString, parameters) {
			var   newString = ''
				, index = 1
				, offset = 0
				, len = 0
				, keys
				, reg
				, result;


			reg = /\?/gi;
			len = parameters.length;

			while(result = reg.exec(SQLString)){
				if (parameters.length >= index) {
					newString += SQLString.substring(offset, result.index) + (parameters[index -1] === null ? null : this._escape(this._toString(parameters[index -1])));
					index++;
					offset = result.index + 1;
				}
				else {
					log.error('InvalidSQLParametersException:');
					log(SQLString, parameters);
					throw new Error('There are not enough aprameters to fill in the placeholders in the SQL query!').setName('InvalidSQLParametersException');
				}
			}

			if (offset < SQLString.length) newString += SQLString.substr(offset);
			
			return newString;
		}



		/**
		 * the _connected() method gets called when a connection could be established
		 */
		, _connected: function() {
			this.emit('connected');
		}


		/**
		 * the _disconnected() method gets called when the conenciton was closed
		 */
		, _disconnected: function() {
			this.emit('disconnected');
		}

		/**
		 * the _error() method gets called when an error occured
		 *
		 * @param <Error> error
		 */
		, _error: function(err) {
			this.emit('error', err);
		}

		/**
		 * the _end() method gets called when the conenctions ends
		 */
		, _end: function(err) {
			if (!this._ended) {
				this._ended = true;
				this._setBusy();
				this.emit('end', err);

				// remove all eventlisteners
				this.off();
			}			
		}


		/*
		 * emit the busy event if it wasn't emitted already
		 */
		, _setBusy: function() {
			if (this._idle && !this._dontPool) {
				this._idle = false;
				this.emit('busy');
			}
		}


		/*
		 * emit the idle event if it wasn't emitted already
		 */
		, _setIdle: function() {
			//log.warn('%s, dile, %s, transaction %s, _dontPool %s, condition %s', this.id, this._idle, this.isTransaction, this._dontPool, !this._idle && !this.isTransaction && !this._dontPool);
			// don't ever mark transactions as idle
			if (!this._idle && !this.isTransaction && !this._dontPool) {
				this._idle = true;
				this.emit('idle');
			}
		}



		/**
		 * the _checkInterface() method checks if all required methods
		 * on are implemented
		 *
		 */
		, _checkInterface: function() {
			if (!type.function(this._connect)) throw new Error('The Connection implementation must implement the «instance._connect(done)» method!').setName('InterfaceException');
			if (!type.function(this._query)) throw new Error('The Connection implementation must implement the «instance._query(sql, callback)» method!').setName('InterfaceException');
			if (!type.function(this._describe)) throw new Error('The Connection implementation must implement the «_describe» method!').setName('InterfaceException');
			if (!type.function(this._escape)) throw new Error('The Connection implementation must implement the «_escape» method!').setName('InterfaceException');
			if (!type.function(this._escapeId)) throw new Error('The Connection implementation must implement the «_escapeId» method!').setName('InterfaceException');
			if (!type.function(this._render)) throw new Error('The Connection implementation must implement the «_render» method!').setName('InterfaceException');
			if (!type.function(this._canBleed)) throw new Error('The Connection implementation must implement the «_canBleed» method!').setName('InterfaceException');
			if (!type.function(this._toString)) throw new Error('The Connection implementation must implement the «_toString» method!').setName('InterfaceException');
			if (!type.function(this._toType)) throw new Error('The Connection implementation must implement the «_toType» method!').setName('InterfaceException');
			if (!type.function(this.lock)) throw new Error('The Connection implementation must implement the «lock» method!').setName('InterfaceException');
		}
	});
}();

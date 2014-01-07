!function(){

	var   Class 		= require('ee-class')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, arg 			= require('ee-arguments')
		, log 			= require('ee-log');



	module.exports = new Class({
		inherits: EventEmitter

		/**
		 * class constructor, must be called from inheriting class
		 *
		 * @param <Object> connection configuration
		 */
		, init: function(options) {
			this.options = options;

			// check if the interface is correct implemented
			this._checkInterface();
			this._connect(function(err) {
				this.emit('load', err);

				if (err) {
					this._disconnected();
					this._error(err);
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
		, query: function(){
			var   sql 			= arg(arguments, 'string', null)
				, parameters 	= arg(arguments, 'object', arg(arguments, 'array'))
				, callback 		= arg(arguments, 'function', function(){})
				, readOnly 		= arg(arguments, 'boolean', true)
				, canBleed 		= false
				, SQLString
				, data;


			// render object query if required
			if (!sql){
				data = this._render(parameters);
				sql = data.sql;
				parameters = data.parameters;
			}

			// insert variables into sql
			SQLString = this._fillSQL(sql, parameters);

			// check if the query contains statements which may bleed into another query
			canBleed = this._canBleed(SQLString);

			// execute the sql statement
			this._query(SQLString, callback);
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
				result = this.escape(this._toString(parameters[keys[l]]));
				SQLString = SQLString.replace(new RegExp('?'+keys[l], 'gi'), result);
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
					newString += result.substring(offset, result.index) + this.escape(this._toString(parameters[index -1]));
					index++;
					offset = result.index + 1;
				}
				else {
					log.error('InvalidSQLParametersException:');
					log(SQLString, parameters);
					throw new Error('There are not enough aprameters to fill in the placeholders in the SQL query!').setName('InvalidSQLParametersException');
				}
			}

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
		}
	});
}();

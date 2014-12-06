!function(){

    var   Class         = require('ee-class')
        , EventEmitter  = require('ee-event-emitter')
        , type          = require('ee-types')
        , arg           = require('ee-arguments')
        , async         = require('ee-async')
        , argv          = require('ee-argv')
        , log           = require('ee-log');


    var   dev       = argv.has('dev-db')
        , toString  = Object.prototype.toString;


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

        // mode map indicates if a query is readonly
        , _modeMap: {
              query: true
            , insert: false
            , update: false
            , delete: false
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


      

        /*
         * execute a query on this connection, takes several configurations
         *
         * @param <Object> query configuration
         *                 - SQL            <String>    optional    sql query
         *                 - query          <Object>    optional    query definition
         *                 - parameters     <Object>    optional    query parameters, optional
         *                 - debug          <Boolean>   optional    debnug this query
         *                 - readOnly       <Boolean>   optional    is the query readonly?
         *                 - mode           <String>    optional    query mode, defaults to «query»
         *                 - callback       <Function>  optional    callback
         */
        , query: function(configuration) {
            var callback;

            if (type.array(configuration)) throw new Error('No longer supporting multiple queries!');
            if (type.array(configuration.parameters)) throw new Error('No longer supporting query parameters as array!');

            // wre busy now, don't let others use this conenction
            this._setBusy();


            // create a sql query from a query object
            if (configuration.query) {  
                // abort if it returns false, it will have triggered the callback already
                if (!this._render(configuration)) return;
            }


            // do the query parameterization
            this._paramterizeQuery(configuration);

            // replace callback
            callback = configuration.callback;
            configuration.callback = function(err, data) {
                // return 
                if (callback) callback(err, data);

                // let otherrs use the conenction again
                this._setIdle();
            }.bind(this);


            // execute the query
            this._query(configuration);
        }


        
        /*
         * renders an SQL string from a query object
         *
         * @param <Object> query object
         */
        , _render: function(configuration) {
            var result;

            // default mode
            if (!configuration.mode) configuration.mode = 'query';

            // check for valid mode
            if (!this._validModes[configuration.mode]) {
                if (configuration.callback) configuration.callback(new Error('Invalid quer mode «'+configuration.mode+'» !'));
                return false;
            }
            else {
                // render
                result = this._querBuilder._render(configuration.mode, configuration.query);

                // set mode
                if (!type.boolean(configuration.readOnly)) configuration.readOnly = this._modeMap[configuration.mode];

                // store rendered query on query configuration
                configuration.SQL           = result.SQLString.trim()+';';
                configuration.parameters    = result.parameters;

                return true;
            }
        }



        /*
         * debug banner generator
         *
         * @param <String> title for the banner
         * @param <Boolean> is this the starting or endingbanner?
         *
         * @returns <String> banner
         */
        , _createDebugBanner: function(title, endBanner) {
            var   len       = Math.floor((60-title.length)/2)
                , boundary  = Array.apply(null, {length: len}).join(endBanner ? '▲' : '▼');

            return boundary+' '+title.toUpperCase()+' '+boundary;
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


        /*
         * execute a query without the advanced stuff
         * deprecated!
         */
        , queryRaw: function() {
            var configuration = {};

            configuration.SQL           = arg(arguments, 'string');
            configuration.parameters    = arg(arguments, 'array', arg(arguments, 'object', {}));
            configuration.callback      = arg(arguments, 'function');
            configuration.readOnly      = arg(arguments, 'boolean', true);

            // use the normal query mode
            this.query(configuration);
        }




        , describe: function(databases, callback){
            this._describe(databases, callback);
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

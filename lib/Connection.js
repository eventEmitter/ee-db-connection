(function() {
    'use strict';


    var   Class         = require('ee-class')
        , EventEmitter  = require('ee-event-emitter')
        , argv          = require('ee-argv')
        , log           = require('ee-log')
        , RelatedError  = require('related-error')
        , asyncMethod   = require('async-method')
        ;



    var   dev           = argv.has('dev-db')
        , debug         = argv.has('debug-sql') || process.env.debug_sql === true
        , debugErrors   = argv.has('debug-sql-errors')
        , debugSlow     = argv.has('debug-slow-queries')
        , slowDebugTime = debugSlow && type.string(argv.get('debug-slow-queries')) ? argv.get('debug-slow-queries') : 200;




    module.exports = new Class({
        inherits: EventEmitter

        // if the conenction is idle it can be queried
        // if its not idle an attempt to query will 
        // throw an exeption
        , _idle: false

        // public getter and setter 
        , idle: {
              get: function() { return this._idle;}
            , set: function(value) {
                value = !!value;

                // check if the status has changed
                if (value !== this._idle) {
                    this._idle = value;

                    // dont emit the events if we're not part of the pool
                    if (this.pooled) this.emit(value ? 'idle' : 'busy');
                }
            }
        }



        // indicates if this conenction is still part of the pool
        // if yes, it can be returned to it after any readonly query
        , pooled: true


        // indeicates if the conenction has ended
        , ended: false


        // if this is set to true we have to end this connection asap!
        , dead: false


        // the query timeout
        , timeout: 30000


        // the connect timeout
        , connectTimeout: 15000


        // enable the timeout also on writing queries?
        , timeoutOnWrite: false


        // indicates if this conenction is part of a transaction
        , isTransaction: false


        // indicates if the transaction is open
        , transactionOpen: false


        // indicates if a query is running
        , queryRunning: false



        // differnet locks that can be obtained by the user on a transaction
        // the orm does its best to get a consistent behavior across the different
        // rdbms systems, which is sadly not possible with locks.
        // see the lock description inside the specific driver
        , LOCK_READ:        {enumerable: true, value: 'LOCK_READ'}
        , LOCK_WRITE:       {enumerable: true, value: 'LOCK_WRITE'}
        , LOCK_EXCLUSIVE:   {enumerable: true, value: 'LOCK_EXCLUSIVE'}




        // the queries have a mode under which they are executed
        // this is a map of all the valid modes
        , validModes: {
              query: true
            , insert: true
            , update: true
            , delete: true
            , create: true
            , alter: true
            , drop: true
            , grant: true
            , index: true
            , transaction: true
            , lock: true
        }




        /**
         * class constructor
         *
         * @param {object} connection config
         * @oaram {string} id connection identifier
         */
        , init: function(config, id) {

            // unique id, used for debugging
            // and the linked lists in the pool
            // implementation
            this.id = Symbol(id);


            // store config for later use
            this.config = config;


            // if a connection error occures we need
            // to reset the queryRunning flag
            this.on('error', function() {
                this.queryRunning = false;
            }.bind(this));

            // this is the case too if the connection ends ;)
            this.on('end', function() {
                this.queryRunning = false;
            }.bind(this));
        }





        /**
         * the outside may initiate the connection, so it
         * gets proper feedback
         */
        , connect: function() {

            return new Promise(function(resolve, reject) {
                var hasTimeout = false;



                // we're not waiting too long for a connection
                // to be established
                var connectTimeout = setTimeout(function() {

                    // remove all event listeners
                    this.off();

                    // make sure that the connection gets closed 
                    // as soon it may connect
                    hasTimeout = true;

                    // connect failed
                    reject(new RelatedError.FailedToConnectError(new Error("Encoutered a connect timeout!")));
                }.bind(this), this.connectTimeout);




                // let the driver specific method connect
                this.driverConnect(config, function(err) {
                    if (hasTimeout) {

                        // the connection had a timeout, let the driver kill it!
                        this.endConnection(function() {});
                    }
                    else {

                        // clear the timeout timer
                        clearTimeout(connectTimeout);


                        if (!err) {
                            // ok, connected
                            resolve();

                            // wait a tick, so that all relevant code gets this event
                            process.nextTick(function() {
                                this.idle = true;
                            }.bind(this));
                        }
                        else {
                            if (dev) log.warn('Failed to establish connection to the database: '+err, err);

                            // remove all event listeners
                            this.off();

                            // return
                            reject(err);
                        }
                    }
                }.bind(this));
            }.bind(this));
        }





        /**
         * returns the db confog
         *
         * @returnd {object} config
         */
        , getConfig: function() {
            return this.config;
        }




        /*
         * execute a query on this connection, takes several configurations
         *
         * @param <Object> query configuration
         *                 - SQL            <String>    optional    sql query
         *                 - query          <Object>    optional    query definition
         *                 - parameters     <Object>    optional    query parameters, optional
         *                 - debug          <Boolean>   optional    debug this query
         *                 - mode           <String>    optional    query mode, defaults to «query»
         *                 - callback       <Function>  optional    callback
         */
        , query: asyncMethod(function(query, callback) {
            if (this.ended) callback(new Error('Cannot query, the connection has ended!'));
            else if (!query.mode) callback(new Error('Missing the query mode!'));
            else if (!this.validModes[query.mode]) callback(new Error('Invaldi query mode «'+query.mode+'»!'));
            else {

                // mark as busy 
                this.idle = false;


                // create a sql query from a query object
                if (query.query) this.render(query);


                // do the query parameterization
                this.paramterizeQuery(query);


                // indicate that a query is beeing executed
                this.queryRunning = true;


                // execute the query
                this.executeQuery(query, function(err, data) {

                    // return results
                    callback(err, data);

                    // not running anything
                    this.queryRunning = false;


                    // if there is an error we may need to check
                    // if its a connectivity issue, if its one
                    // the node may be shut down
                    if (err && err instanceof RelatedError.FailedToConnectError) {
                        this.end(err);
                    }
                    else {
                        // we're idle again
                        this.idle = true;
                    }
                }.bind(this));
            }            
        })






        /**
         * removed the conenction form the pool
         */
        , removeFromPool: function() {
            if (this.ended) return callback(new Error('Cannot remove from pool, the connection has ended!'));

            if (this.pooled) {
                if (dev) log.highlight('removed connection from pool ...');

                // this connection is now busy
                this.idle = false;

                // this connection msut not be returned to the pool
                this.pooled = false;

                // tell the node
                this.emit('poolRemove');
            }
            else throw new Error('Cannot remove connection from pool, it was removed already!');
        }






        /**
         * starts a transaction
         */
        , createTransaction: asyncMethod(function(callback) {
            if (this.ended) return callback(new Error('Cannot start transaction, the connection has ended!'));

            if (dev) log.highlight('started transaction ...');

            // tell everone that we're a transactio noe
            this.isTransaction = true;

            // the transaction is open from now on
            this.transactionOpen = true;

            // we're busy
            this.idle = false;

            // conenction should not be returned to the pool
            this.pooled = false;

            // execute query
            this.query({SQL: 'start transaction;', mode: 'transaction'}, callback);
        })






        /**
         * commit an open transaction
         */
        , commit: asyncMethod(function(callback) {
            if (!this.isTransaction) return callback(new Error('Cannot commit, this is no transaction!'));
            if (!this.transactionOpen) return callback(new Error('Cannot commit, the transaction has ended already!'));
            if (this.ended) return callback(new Error('Cannot commit, the connection has ended!'));


            // query
            this.query({SQL: 'commit;', mode: 'transaction'}, function(err) {
                if (dev) log.highlight('commited transaction'+(err ? ': error ('+err.message+')' : '')+' ...');

                // return to the user
                callback(err);

                // kill the conenction
                this.end();
            }.bind(this));
        })





        /**
         * roll back an open transaction
         */
        , rollback: asyncMethod(function(callback) {
            if (!this.isTransaction) return callback(new Error('Cannot rollback, this is no transaction!'));
            if (!this.transactionOpen) return callback(new Error('Cannot rollback, the transaction has ended already!'));
            if (this.ended) return callback(new Error('Cannot rollback, the connection has ended!'));

            // query
            this.query({SQL: 'rollback;', mode: 'transaction'}, function(err) {
                if (dev) log.highlight('rolled back transaction'+(err ? ': error ('+err.message+')' : '')+' ...');

                // return to the user
                callback(err);

                // kill the conenction
                this.end();
            }.bind(this));
        })




        /**
         * call the driver specific describe implementation
         */
        , describe: function(databases, callback){
            this._describe(databases, callback);
        }





        /**
         * close the connection
         */
        , end: function(err) {
            if (!this.ended) {
                this.ended = true;

                // triggers a connectivity check on the main node
                if (err && err instanceof RelatedError.FailedToConnectError) this.emit('connectivityProblem', true);

                // let the outside know
                // that we're finished
                this.emit('end', err);

                // remove from pool
                this.idle = false;

                // end if there is still a conenction
                if (this.connection) {

                    // the driver must end the connection now
                    this.endConnection(this.off.bind(this));
                }
                else this.off();
            }
        }





        /**
         * records certain data that can be used to debug the 
         * queries
         *
         * @param {object} query the query definition
         *
         */
        , debug: function(query) {
            if (debug || debugSlow || query.debug) query.start = Date.now();

            if (debugErrors || debug) {
                oldLimit = Error.stackTraceLimit;
                Error.stackTraceLimit = Infinity;
                query.stack = new Error('stacktrace');
                Error.stackTraceLimit = oldLimit;
            }
        }



        /**
         * prints all sorts of debug information
         *
         * @para {query} query the quuery object
         */
        , printDebugInfo: function(query, err, rows) {

            // debug logging
            if (debug || query.debug || (debugSlow && (Date.now()-start) > slowDebugTime) || (debugErrors && err)) {
                // capture query time
                time = Date.now()-query.start;
                logStr = '['+this.brand+']['+this.id+'] ';

                // banner
                log.debug(logStr+this.createDebugBanner(debug || query.debug ? 'QUERY DEBUGGER' : 'SLOW QUERY'));

                // status
                if (err) log.error(logStr+'The query failed: '+err);
                else log.debug(logStr+'Query returned '.grey+((rows ? rows.length : 0)+'').yellow+' rows'.white+' ('.grey+((Date.now()-start)+'').yellow+' msec'.white+') ...'.grey);

                // query
                log.debug(logStr+this.renderSQLQuery(query.SQL, query.values).white);

                // trace
                if (err && query.stack) {
                    log.info('Stacktrace:');
                    log(query.stack);
                }

                // end banner
                log.debug(logStr+this.createDebugBanner((debug || query.debug ? 'QUERY DEBUGGER' : 'SLOW QUERY'), true));
            }

            if (err) {
                err.sql = this.renderSQLQuery(query.SQL, query.values);
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
        , createDebugBanner: function(title, isEndBanner) {
            var   len       = Math.floor((60-title.length)/2)
                , boundary  = Array.apply(null, {length: len}).join(isEndBanner ? '▲' : '▼');

            return boundary+' '+title.toUpperCase()+' '+boundary;
        }
    });
})();

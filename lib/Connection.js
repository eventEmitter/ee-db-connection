(function() {
    'use strict';


    var   Class         = require('ee-class')
        , EventEmitter  = require('ee-event-emitter')
        , argv          = require('ee-argv')
        , log           = require('ee-log')
        , asyncMethod   = require('async-method')
        ;



    var debug = argv.has('dev-db');





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


        // the query timeout
        , timeout: 30000


        // enable the timeout also on writing queries?
        , timeoutOnWrite: false


        // indicates if this conenction is part of a transaction
        , isTransaction: false


        // indicates if the transaction is open
        , transactionOpen: false



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


            // automatically connect, this is done
            // by the specific driver
            this.connect(config, function(err) {
                if (!err) this.idle = true;
                else {
                    if (debug) log.warn('Failed to establish connection to the database: '+err, err);
                    this.end(err);
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


                // execute the query
                this.executeQuery(query, function(err, data) {
                    callback(err, data);

                    this.idle = true;
                }.bind(this));
            }            
        })






        /**
         * removed the conenction form the pool
         */
        , removeFromPool: function() {
            if (this.ended) return callback(new Error('Cannot remove from pool, the connection has ended!'));

            if (debug) log.highlight('removed connection from pool ...');

            // this connection is now busy
            this.idle = false;

            // this connection msut not be returned to the pool
            this.pooled = false;
        }






        /**
         * starts a transaction
         */
        , createTransaction: asyncMethod(function(callback) {
            if (this.ended) return callback(new Error('Cannot start transaction, the connection has ended!'));

            if (debug) log.highlight('started transaction ...');

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
                if (debug) log.highlight('commited transaction'+(err ? ': error ('+err.message+')' : '')+' ...');

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
                if (debug) log.highlight('rolled back transaction'+(err ? ': error ('+err.message+')' : '')+' ...');

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

                // let the outside know
                // that we're finished
                this.emit('end', err);

                // remove from pool
                this.idle = false;

                // end if there is still a conenction
                if (this.connection) {
                    this.connection.on('end', this.off.bind(this));

                    // end theconnection
                    this.connection.end();

                    // delete the conenction refernece, no one should be able to use it anymore!
                    delete this.connection;
                }
                else this.off();
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
    });
})();

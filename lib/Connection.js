(function() {
    'use strict';


    const Class                 = require('ee-class');
    const EventEmitter          = require('ee-event-emitter');
    const argv                  = require('ee-argv');
    const log                   = require('ee-log');
    const RelatedError          = require('related-error');
    const QueryContext          = require('related-query-context');
    const sqlformatter          = require('sqlformatter');
    const relatedSlow           = argv.has('related-slow');
    const relatedSQL            = argv.has('related-sql');
    const relatedPreSQLQuery    = argv.has('related-pre-query-sql');
    const relatedErrors         = argv.has('related-errors');




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
                    if (this.pooled) {
                        if (value) {
                            // don't emit idle if the connection has ended or was killed
                            if (!this.ended && !this.killed) {
                                process.nextTick(() => {
                                    this.emit('idle');
                                });
                            }
                        }
                        else this.emit('busy');
                    }
                }
            }
        }




        // private flag, indicates if a query is running
        , _queryRunning: false

        // indicates how many queries are running
        , _runningQueries: 0

        // this status is used to kill the conenction if required
        , queryRunning: {
              get: function() {return this._queryRunning;}
            , set: function(status) {
                if (status) {
                    this._runningQueries++;
                    this._queryRunning = true;
                }
                else {
                    this._runningQueries--;

                    if (this._runningQueries <= 0) {
                        this._queryRunning = false;
                        
                        // emit event, so that the conenction can be terminated if it was killed
                        this.emit('queryEnd');
                    }
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






        /**
         * class constructor
         *
         * @param {object} connection config
         * @oaram {string} id connection identifier
         * @param {object} node the node this connection belongs to
         */
        , init: function(config, id, node) {

            // unique id, used for debugging
            // and the linked lists in the pool
            // implementation
            this.id = Symbol(id);



            // our pools
            this.pools = node ? node.pools : [];



            // store config for later use
            this.config = config;


            // if a connection error occures we need
            // to reset the queryRunning flag
            this.on('error', () => {
                this._runningQueries = 0;
                this.queryRunning = false;
            });

            // this is the case too if the connection ends ;)
            this.on('end', () => {
                this._runningQueries = 0;
                this.queryRunning = false;
            });
        }





        /**
         * the node may kill all connections if it thinks the host
         * has gone offline. we have to make sure that the connection 
         * is terminated as soon as possible
         */
        , kill: function() {
            this.killed = true;


            if (this.idle) {

                // mark as busy
                this.idle = false;
                
                // idle, not a transcation
                this.end();

                // remove lsitners
                this.off();
            }
            else if (this.queryRunning) {

                // wait for the running query, but only if its not a transaction
                // transactions will kill themselves as soon as they are finished
                if (!this.isTransaction) {
                    this.once('queryEnd', () => {
                        
                        this.end();
                        
                        // remove lsitners
                        this.off();
                    });
                }
            } 
            else {
                this.end();
                
                // remove listenerss
                this.off();
            }
        }






        /**
         * the outside may initiate the connection, so it
         * gets proper feedback
         */
        , connect: function() {

            return new Promise((resolve, reject) => {
                let hasTimeout = false;

                // we're not waiting too long for a connection
                // to be established
                let connectTimeout = setTimeout(() => {

                    // remove all event listeners
                    this.off();

                    // make sure that the connection gets closed 
                    // as soon it may connect
                    hasTimeout = true;

                    // connect failed
                    reject(new RelatedError.FailedToConnectError(new Error("Encoutered a connect timeout!")));
                }, this.connectTimeout);




                // let the driver specific method connect
                this.driverConnect(this.config, (err) => {

                    if (this.killed) {
                        // the connection was killed during startup
                        // end everything now
                        if (!hasTimeout) clearTimeout(connectTimeout);

                        this.endConnection(() => {});
                    }
                    else {
                        if (hasTimeout) {

                            // the connection had a timeout, let the driver kill it!
                            this.endConnection(() => {});
                        }
                        else {

                            // clear the timeout timer
                            clearTimeout(connectTimeout);


                            if (!err) {
                                // ok, connected
                                resolve();

                                // wait a tick, so that all relevant code gets this event
                                this.idle = true;
                            }
                            else {

                                // remove all event listeners
                                this.off();

                                // return
                                reject(err);
                            }
                        }
                    }
                });
            });
        }





        /**
         * returns the db confog
         *
         * @returnd {object} config
         */
        , getConfig: function() {
            return this.config;
        }








        /**
         * execute a query
         */
        , query: function(queryContext, values) {
            if (this.ended || this.killed) return Promise.reject(new Error('Cannot execute the query, the connection has ended!'));
            else {

                // the user may also use plain sql instead of
                // a query context
                if (typeof queryContext === 'string') {
                    queryContext = new QueryContext({
                          sql       : queryContext
                        , values    : values
                    });
                }


                // mark as busy 
                this.idle = false;

                // indicate that a query is beeing executed
                this.queryRunning = true;


                // check if the query context is a valid object
                if (!queryContext || typeof queryContext.isValid !== 'function') {
                    log(queryContext);
                    return Promise.reject(new Error(`The queryContext is invalid!`));
                } else if (!queryContext.isValid()) {


                    // check if the context is valid
                    return Promise.reject(new Error(`The query is invalid: ${queryContext.invalidBecauseOf()}`));
                } else {


                    // tell the context the it is now beeing executed
                    queryContext.setStatus('before-execute');


                    if (relatedPreSQLQuery) this.printPreQueryDebugInfo(queryContext);


                    // execute the query
                    return this.executeQuery(queryContext).then((data) => {

                        // set query status
                        queryContext.setStatus('after-execute');


                        // not running anything
                        this.queryRunning = false;

                        // return to the pool
                        if (!this.isTransaction) this.idle = true;


                        // do the debugging stuff
                        if (queryContext.debug || relatedSQL || (relatedSlow && queryContext.getExecutionTime() > (argv.get('related-slow') ? argv.get('related-slow') : 200))) this.printDebugInfo(queryContext, data);



                        // return the results
                        return Promise.resolve(data);
                    }).catch((err) => {

                        // set status
                        queryContext.setStatus('after-execute', err);


                        // not running anything
                        this.queryRunning = false;

                        // maybe we should end the connection
                        if (err && err instanceof RelatedError.FailedToConnectError) this.end(err);
                        else if (!this.isTransaction) this.idle = true;



                        if (queryContext.debug || relatedErrors || relatedSQL || (relatedSlow && queryContext.getExecutionTime() > (argv.get('related-slow') ? argv.get('related-slow') : 200))) this.printDebugInfo(queryContext);


                        // return the error
                        return Promise.reject(err);
                    });
                }
            }
        }






        /**
         * debug info printer
         */
        , printDebugInfo: function(queryContext, data) {
            let sql = sqlformatter.format(this.renderSQLQuery(queryContext.sql, queryContext.values));

            console.log(`
    ${this.createDebugBanner('SQL DEBUGGER').grey}
    ${'Execution Time:'.white} ${(queryContext.getExecutionTime()+'').green.bold}${', Waiting Time:'.white} ${(queryContext.getWaitTime()+'').green.bold}${data && data.length ? ', Rows Returned: '.white+(data.length+'').green.bold : ''} ${queryContext.err ? '\n\n    The Query Failed: '.yellow+queryContext.err.message.white : ''}

    ${sql.trim().blue.bold}
    
    ${this.brand.grey} ${this.id.toString().grey} ${'['.grey}${this.pools.join(', ').white}${']'.grey}
    ${this.createDebugBanner('SQL DEBUGGER', true).grey}
            `);
        }






        /**
         * debug info printer
         */
        , printPreQueryDebugInfo: function(queryContext) {
            let sql = sqlformatter.format(this.renderSQLQuery(queryContext.sql, queryContext.values));

            console.log(`
    ${this.createDebugBanner('PRE QUERY SQL DEBUGGER').grey}
    
    ${sql.trim().blue.bold}
    
    ${this.brand.grey} ${this.id.toString().grey} ${'['.grey}${this.pools.join(', ').white}${']'.grey}
    ${this.createDebugBanner('PRE QUERY SQL DEBUGGER', true).grey}
            `);
        }





        /*
         * debug banner generator
         *
         * @param <String> title for the banner
         * @param <Boolean> is this the starting or endingbanner?
         *
         * @returns <String> banner
         */
        , createDebugBanner: function(title, endBanner) {
            var   len       = Math.floor((100-title.length)/2)
                , boundary  = Array.apply(null, {length: len}).join(endBanner ? '▲' : '▼');

            return boundary+' '+title.toUpperCase()+' '+boundary;
        }







        /**
         * removed the conenction form the pool
         */
        , removeFromPool: function() {
            if (this.ended || this.killed) return callback(new Error('Cannot remove the connection from the pool, the connection has ended!'));

            if (this.pooled) {

                // this connection is now busy
                this.idle = false;

                // this connection msut not be returned to the pool
                this.pooled = false;

                // tell the node
                this.emit('poolRemove');
            }
        }









        /**
         * starts a transaction
         */
        , createTransaction: function() {
            if (this.ended || this.killed) return Promise.reject(new Error('Cannot start transaction, the connection has ended!'));

            // tell everone that we're a transactio noe
            this.isTransaction = true;

            // the transaction is open from now on
            this.transactionOpen = true;

            // conenction should not be returned to the pool
            this.removeFromPool();

            // execute query
            return this.query(new QueryContext({sql: 'start transaction;', mode: 'transaction'}));
        }








        /**
         * commit an open transaction
         */
        , commit: function() {
            if (!this.isTransaction) return Promise.reject(new Error('Cannot commit, this is no transaction!'));
            if (!this.transactionOpen) return Promise.reject(new Error('Cannot commit, the transaction has ended already!'));
            if (this.ended) return Promise.reject(new Error('Cannot commit, the connection has ended!'));


            // query
            return this.query(new QueryContext({sql: 'commit;', mode: 'transaction'})).then((data) => {


                // kill the connection
                this.end(this);


                // return results
                return Promise.resolve(data);
            });
        }








        /**
         * roll back an open transaction
         */
        , rollback: function() {
            if (!this.isTransaction) return Promise.reject(new Error('Cannot rollback, this is no transaction!'));
            if (!this.transactionOpen) return Promise.reject(new Error('Cannot rollback, the transaction has ended already!'));
            if (this.ended) return Promise.reject(new Error('Cannot rollback, the connection has ended!'));

            // query
            return this.query(new QueryContext({sql: 'rollback;', mode: 'transaction'})).then((data) => {


                // kill the conenction
                this.end();

                // return results
                return Promise.resolve(data);
            }).catch((err) => {

                this.end(err);

                return Promise.reject(err);
            });
        }

    








        /**
         * close the connection
         */
        , end: function(err) {
            if (!this.ended) {
                this.ended = true;

                // dereference the node
                this.pools = null;

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
    });
})();

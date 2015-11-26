(function() {
    'use strict';


    var   Class         = require('ee-class')
        , type          = require('ee-types')
        , log           = require('ee-log')
        , Connection    = require('../lib/Connection')
        ;





    module.exports = new Class({
        inherits: Connection


        , brand: 'TEST'


        , pools : ['read', 'write']

        /**
         * establishes the db conenction
         */
        , driverConnect: function(config, callback) {
            process.nextTick(callback);
        }



        /**
         * executes a query
         */
        , executeQuery: function(queryContext) {
            return Promise.resolve();
        }



        , renderSQLQuery: function(input) {
            return input;
        }
    });
})();

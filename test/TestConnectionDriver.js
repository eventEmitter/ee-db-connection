(function() {
    'use strict';


    var   Class         = require('ee-class')
        , type          = require('ee-types')
        , log           = require('ee-log')
        , asyncMethod   = require('async-method')
        , Connection    = require('../lib/Connection')
        ;





    module.exports = new Class({
        inherits: Connection



        /**
         * establishes the db conenction
         */
        , connect: function(config, callback) {
            process.nextTick(callback);
        }



        /**
         * executes a query
         */
        , executeQuery: function(query, callback) {
            process.nextTick(callback);
        }


        /**
         * takes a query, its parameters and inserts the into the query
         */
        , paramterizeQuery: function() {

        }


        /**
         * convert a query object to an sql string
         */
        , render: function() {
            return {
                  SQL: ''
                , parameters: {}
            };
        }
    });
})();

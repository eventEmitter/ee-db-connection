(function() {
	'use strict';
	
	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, assert 		= require('assert');



	var   TestConenction = require('./TestConnectionDriver')
		, idleCount = 0
		, busyCount = 0
		, config = {}
		, connection;





    describe('The Connection', function() {

        it('should emit the idle event if the conenction could be established', function(done) {
            connection = new TestConenction(config, 1);

            var i = 0, doneCall = () => {
            	if (++i === 2) done();
            }

            connection.connect().then(doneCall).catch(done);

            connection.on('idle', function() {idleCount++;});
            connection.on('busy', function() {busyCount++;});

            connection.once('idle', doneCall);
        });


        it('should accept raw sql queries', function(done) {
            connection.query({SQL: 'select 1;', mode: 'query'}, done);
        });


        it('should accept queries', function(done) {
            connection.query({query: {
                  select: ['*']
                , from: 'event'
                , database: 'related_test_postgres'
            }, mode: 'query'}, done);
        });


        it('should create a transaction', function(done) {
            connection.createTransaction(done);
        });


        it('should commit a transaction', function(done) {
            connection.commit(done);
        });


        it('should not accept any queries anymore', function(done) {
            connection.query({SQL: 'select 1;', mode: 'query'}, function(err) {
                assert(err instanceof Error);
                done();
            });
        });


        it('should have emitted the correct amount of events', function() {
            assert(idleCount === 3);
            assert(busyCount === 3);
        });


        it('should return the correct config object', function() {
            assert(typeof connection.getConfig() === 'object');
        });




        it('should kill itself if asked to do so', function(done) {
            let connection = new TestConenction(config, 1);

            connection.connect();

            connection.on('end', done);

            connection.kill();
        });
    });
})();
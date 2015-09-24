(function() {
	'use strict';
	
	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, assert 		= require('assert');



	var   TestConenction = require('./TestConnectionDriver')
		, idleCount = 0
		, busyCount = 0
		, connection;




	describe('The Connection', function() {

		it('should emit the idle event if the conenction could be established', function(done) {
			connection = new TestConenction({}, 1);

			connection.on('idle', function() {idleCount++;});
			connection.on('busy', function() {busyCount++;});

			connection.once('idle', done);
		});


		it('should accept raw sql queries', function(done) {
			connection.query({SQL: 'select 1;', mode: 'query'}, done);
		});


		it('should accept queries', function(done) {
			connection.query({query: {}, mode: 'insert'}, done);
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
	});
})();
	
const assert = require('assert');
const QueryContext = require('related-query-context');



const TestConenction = require('./TestConnectionDriver');
let idleCount = 0;
let busyCount = 0;
let config = {};
let connection;





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
        connection.query(new QueryContext({sql: 'select 1;', mode: 'select'})).then(done).catch(done);
    });


    it('should not accept non SQL queries', function(done) {
        connection.query(new QueryContext({query: {
                select: ['*']
            , from: 'event'
            , database: 'related_test_postgres'
        }, mode: 'select'})).then(() => {
            done(new Error('Expected the query to fail!'))
        }).catch((err) => {
            assert(err instanceof Error);
            done();
        });
    });


    it('should create a transaction', function(done) {
        connection.createTransaction().then(done).catch(done);
    });


    it('should commit a transaction', function(done) {
        connection.commit().then(done).catch(done);
    });


    it('should not accept any queries anymore', function(done) {
        connection.query(new QueryContext({sql: 'select 1;', mode: 'select'})).then(() => {done()}).catch((err) => {
            assert(err instanceof Error);
            done();
        });
    });


    it('should have emitted the correct amount of events', function() {
        assert.equal(busyCount, 3, 'busyCount');
        assert.equal(idleCount, 3, 'idleCount');
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
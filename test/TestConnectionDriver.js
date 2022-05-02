const Connection = require('../src/Connection.js');



module.exports = class TestConnectionDriver extends Connection {

    brand = 'TEST';


    pools = ['read', 'write'];

    /**
     * establishes the db conenction
     */
    driverConnect(config, callback) {
        process.nextTick(callback);
    }



    /**
     * executes a query
     */
    executeQuery(queryContext) {
        return Promise.resolve();
    }



    renderSQLQuery(input) {
        return input;
    }
}

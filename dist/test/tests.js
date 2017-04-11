"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require("mocha");
var chai_1 = require("chai");
var sinon = require("sinon");
var WebSocket = require("ws");
Object.assign(global, {
    WebSocket: WebSocket,
});
var graphql_1 = require("graphql");
var graphql_subscriptions_1 = require("graphql-subscriptions");
var messageTypes_1 = require("../messageTypes");
var protocols_1 = require("../protocols");
var http_1 = require("http");
var server_1 = require("../server");
var client_1 = require("../client");
var TEST_PORT = 4953;
var KEEP_ALIVE_TEST_PORT = TEST_PORT + 1;
var DELAYED_TEST_PORT = TEST_PORT + 2;
var RAW_TEST_PORT = TEST_PORT + 4;
var EVENTS_TEST_PORT = TEST_PORT + 5;
var ONCONNECT_ERROR_TEST_PORT = TEST_PORT + 6;
var data = {
    '1': {
        'id': '1',
        'name': 'Dan',
    },
    '2': {
        'id': '2',
        'name': 'Marie',
    },
    '3': {
        'id': '3',
        'name': 'Jessie',
    },
};
var userType = new graphql_1.GraphQLObjectType({
    name: 'User',
    fields: {
        id: { type: graphql_1.GraphQLString },
        name: { type: graphql_1.GraphQLString },
    },
});
var schema = new graphql_1.GraphQLSchema({
    query: new graphql_1.GraphQLObjectType({
        name: 'Query',
        fields: {
            testString: { type: graphql_1.GraphQLString },
        },
    }),
    subscription: new graphql_1.GraphQLObjectType({
        name: 'Subscription',
        fields: {
            user: {
                type: userType,
                args: {
                    id: { type: graphql_1.GraphQLString },
                },
                resolve: function (_, _a) {
                    var id = _a.id;
                    return data[id];
                },
            },
            userFiltered: {
                type: userType,
                args: {
                    id: { type: graphql_1.GraphQLString },
                },
                resolve: function (_, _a) {
                    var id = _a.id;
                    return data[id];
                },
            },
            context: {
                type: graphql_1.GraphQLString,
                resolve: function (root, args, ctx) {
                    return ctx;
                },
            },
            error: {
                type: graphql_1.GraphQLString, resolve: function () {
                    throw new Error('E1');
                },
            },
        },
    }),
});
var subscriptionManager = new graphql_subscriptions_1.SubscriptionManager({
    schema: schema,
    pubsub: new graphql_subscriptions_1.PubSub(),
    setupFunctions: {
        'userFiltered': function (options, args) { return ({
            'userFiltered': {
                filter: function (user) {
                    return !args['id'] || user.id === args['id'];
                },
            },
        }); },
    },
});
var handlers = {
    onSubscribe: function (msg, params, webSocketRequest) {
        return Promise.resolve(Object.assign({}, params, { context: msg['context'] }));
    },
};
var options = {
    subscriptionManager: subscriptionManager,
    onSubscribe: function (msg, params, webSocketRequest) {
        return handlers.onSubscribe(msg, params, webSocketRequest);
    },
};
var eventsOptions = {
    subscriptionManager: subscriptionManager,
    onSubscribe: sinon.spy(function (msg, params, webSocketRequest) {
        return Promise.resolve(Object.assign({}, params, { context: msg['context'] }));
    }),
    onUnsubscribe: sinon.spy(),
    onConnect: sinon.spy(function () {
        return { test: 'test context' };
    }),
    onDisconnect: sinon.spy(),
};
var onConnectErrorOptions = {
    subscriptionManager: subscriptionManager,
    onConnect: function () {
        throw new Error('Error');
    },
};
function notFoundRequestListener(request, response) {
    response.writeHead(404);
    response.end();
}
var httpServer = http_1.createServer(notFoundRequestListener);
httpServer.listen(TEST_PORT);
new server_1.SubscriptionServer(options, { server: httpServer });
var httpServerWithKA = http_1.createServer(notFoundRequestListener);
httpServerWithKA.listen(KEEP_ALIVE_TEST_PORT);
new server_1.SubscriptionServer(Object.assign({}, options, { keepAlive: 10 }), { server: httpServerWithKA });
var httpServerWithEvents = http_1.createServer(notFoundRequestListener);
httpServerWithEvents.listen(EVENTS_TEST_PORT);
var eventsServer = new server_1.SubscriptionServer(eventsOptions, { server: httpServerWithEvents });
var httpServerWithOnConnectError = http_1.createServer(notFoundRequestListener);
httpServerWithOnConnectError.listen(ONCONNECT_ERROR_TEST_PORT);
new server_1.SubscriptionServer(onConnectErrorOptions, { server: httpServerWithOnConnectError });
var httpServerWithDelay = http_1.createServer(notFoundRequestListener);
httpServerWithDelay.listen(DELAYED_TEST_PORT);
new server_1.SubscriptionServer(Object.assign({}, options, {
    onSubscribe: function (msg, params) {
        return new Promise(function (resolve, reject) {
            setTimeout(function () {
                resolve(Object.assign({}, params, { context: msg['context'] }));
            }, 100);
        });
    },
}), { server: httpServerWithDelay });
var httpServerRaw = http_1.createServer(notFoundRequestListener);
httpServerRaw.listen(RAW_TEST_PORT);
describe('Client', function () {
    var wsServer;
    beforeEach(function () {
        wsServer = new WebSocket.Server({
            server: httpServerRaw,
        });
    });
    afterEach(function () {
        if (wsServer) {
            wsServer.close();
        }
    });
    it('should send INIT message when creating the connection', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(parsedMessage.type).to.equals('init');
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
    });
    it('should send INIT message first, then the SUBSCRIPTION_START message', function (done) {
        var initReceived = false;
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === messageTypes_1.INIT) {
                    connection.send(JSON.stringify({ type: messageTypes_1.INIT_SUCCESS, payload: {} }));
                    initReceived = true;
                }
                if (parsedMessage.type === messageTypes_1.SUBSCRIPTION_START) {
                    chai_1.expect(initReceived).to.be.true;
                    client.unsubscribeAll();
                    done();
                }
            });
        });
        client.subscribe({
            query: "subscription useInfo {\n          user(id: 3) {\n            id\n            name\n          }\n        }",
        }, function (error, result) {
        });
    });
    it('should emit connect event for client side when socket is open', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var unregister = client.onConnect(function () {
            unregister();
            done();
        });
    });
    it('should emit disconnect event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            connectionCallback: function () {
                client.client.close();
            },
        });
        var unregister = client.onDisconnect(function () {
            unregister();
            done();
        });
    });
    it('should emit reconnect event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
            connectionCallback: function () {
                client.client.close();
            },
        });
        var unregister = client.onReconnect(function () {
            unregister();
            done();
        });
    });
    it('should throw an exception when query is not provided', function () {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        chai_1.expect(function () {
            client.subscribe({
                query: undefined,
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
            });
        }).to.throw();
    });
    it('should throw an exception when query is not valid', function () {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        chai_1.expect(function () {
            client.subscribe({
                query: {},
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
            });
        }).to.throw();
    });
    it('should throw an exception when handler is not provided', function () {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        chai_1.expect(function () {
            client.subscribe({
                query: "subscription useInfo($id: String) {\n            user(id: $id) {\n              id\n              name\n            }\n          }",
            }, undefined);
        }).to.throw();
    });
    it('should allow both data and errors on SUBSCRIPTION_DATA', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === messageTypes_1.INIT) {
                    connection.send(JSON.stringify({ type: messageTypes_1.INIT_SUCCESS, payload: {} }));
                }
                if (parsedMessage.type === messageTypes_1.SUBSCRIPTION_START) {
                    connection.send(JSON.stringify({ type: messageTypes_1.SUBSCRIPTION_SUCCESS, id: parsedMessage.id }), function () {
                        var dataMessage = {
                            type: messageTypes_1.SUBSCRIPTION_DATA,
                            id: parsedMessage.id,
                            payload: {
                                data: {
                                    some: 'data',
                                },
                                errors: [{
                                        message: 'Test Error',
                                    }],
                            },
                        };
                        connection.send(JSON.stringify(dataMessage));
                    });
                }
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        client.subscribe({
            query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }, function (error, result) {
            chai_1.expect(result).to.have.property('some');
            chai_1.expect(error).to.be.lengthOf(1);
            done();
        });
    });
    it('should send connectionParams along with init message', function (done) {
        var connectionParams = {
            test: true,
        };
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(JSON.stringify(parsedMessage.payload)).to.equal(JSON.stringify(connectionParams));
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionParams: connectionParams,
        });
    });
    it('should handle correctly init_fail message', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({ type: 'init_fail', payload: { error: 'test error' } }));
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionCallback: function (error) {
                chai_1.expect(error).to.equals('test error');
                done();
            },
        });
    });
    it('should handle init_fail message and handle server that closes connection', function (done) {
        var client = null;
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({ type: 'init_fail', payload: { error: 'test error' } }), function () {
                    connection.close();
                    connection.terminate();
                    setTimeout(function () {
                        chai_1.expect(client.client.readyState).to.equals(WebSocket.CLOSED);
                        done();
                    }, 500);
                });
            });
        });
        client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
    });
    it('should handle correctly init_success message', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({ type: 'init_success' }));
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionCallback: function (error) {
                chai_1.expect(error).to.equals(undefined);
                done();
            },
        });
    });
    it('removes subscription when it unsubscribes from it', function () {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            var subId = client.subscribe({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
            });
            client.unsubscribe(subId);
            chai_1.assert.notProperty(client.subscriptions, "" + subId);
        }, 100);
    });
    it('queues messages while websocket is still connecting', function () {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var subId = client.subscribe({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }, function (error, result) {
        });
        chai_1.expect(client.unsentMessagesQueue.length).to.equals(1);
        client.unsubscribe(subId);
        chai_1.expect(client.unsentMessagesQueue.length).to.equals(2);
        setTimeout(function () {
            chai_1.expect(client.unsentMessagesQueue.length).to.equals(0);
        }, 100);
    });
    it('should call error handler when graphql result has errors', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client.subscribe({
                query: "subscription useInfo{\n          error\n        }",
                variables: {},
            }, function (error, result) {
                if (error) {
                    client.unsubscribeAll();
                    done();
                }
                if (result) {
                    client.unsubscribeAll();
                    chai_1.assert(false);
                }
            });
        }, 100);
        setTimeout(function () {
            subscriptionManager.publish('error', {});
        }, 200);
    });
    it('should call error handler when graphql query is not valid', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client.subscribe({
                query: "subscription useInfo{\n          invalid\n        }",
                variables: {},
            }, function (error, result) {
                if (error) {
                    chai_1.expect(error[0].message).to.equals('Cannot query field "invalid" on type "Subscription".');
                    done();
                }
                else {
                    chai_1.assert(false);
                }
            });
        }, 100);
    });
    function testBadServer(payload, errorMessage, done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === messageTypes_1.SUBSCRIPTION_START) {
                    connection.send(JSON.stringify({
                        type: messageTypes_1.SUBSCRIPTION_FAIL,
                        id: parsedMessage.id,
                        payload: payload,
                    }));
                }
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        client.subscribe({
            query: "\n        subscription useInfo{\n          invalid\n        }\n      ",
            variables: {},
        }, function (errors, result) {
            if (errors) {
                chai_1.expect(errors[0].message).to.equals(errorMessage);
            }
            else {
                chai_1.assert(false);
            }
            done();
        });
    }
    it('should handle missing errors', function (done) {
        var errorMessage = 'Unknown error';
        var payload = {};
        testBadServer(payload, errorMessage, done);
    });
    it('should handle errors that are not an array', function (done) {
        var errorMessage = 'Just an error';
        var payload = {
            errors: { message: errorMessage },
        };
        testBadServer(payload, errorMessage, done);
    });
    it('should throw an error when the susbcription times out', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + DELAYED_TEST_PORT + "/", { timeout: 1 });
        setTimeout(function () {
            client.subscribe({
                query: "subscription useInfo{\n            error\n          }",
                operationName: 'useInfo',
                variables: {},
            }, function (error, result) {
                if (error) {
                    chai_1.expect(error[0].message).to.equals('Subscription timed out - no response from server');
                    done();
                }
                if (result) {
                    chai_1.assert(false);
                }
            });
        }, 100);
    });
    it('should reconnect to the server', function (done) {
        var connections = 0;
        var client;
        var originalClient;
        wsServer.on('connection', function (connection) {
            connections += 1;
            if (connections === 1) {
                originalClient.close();
            }
            else {
                chai_1.expect(client.client).to.not.be.equal(originalClient);
                done();
            }
        });
        client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", { reconnect: true });
        originalClient = client.client;
    });
    it('should resubscribe after reconnect', function (done) {
        var connections = 0;
        var client = null;
        wsServer.on('connection', function (connection) {
            connections += 1;
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === messageTypes_1.SUBSCRIPTION_START) {
                    if (connections === 1) {
                        client.client.close();
                    }
                    else {
                        done();
                    }
                }
            });
        });
        client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", { reconnect: true });
        client.subscribe({
            query: "\n        subscription useInfo{\n          invalid\n        }\n      ",
            variables: {},
        }, function (errors, result) {
            chai_1.assert(false);
        });
    });
    it('should throw an exception when trying to subscribe when socket is closed', function (done) {
        var client = null;
        client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", { reconnect: true });
        setTimeout(function () {
            client.client.close();
        }, 500);
        setTimeout(function () {
            chai_1.expect(function () {
                client.subscribe({
                    query: "\n        subscription useInfo{\n          invalid\n        }\n      ",
                    variables: {},
                }, function (errors, result) {
                });
                done();
            }).to.throw();
        }, 1000);
    });
    it('should throw an exception when the sent message is not a valid json', function (done) {
        setTimeout(function () {
            chai_1.expect(function () {
                var client = null;
                wsServer.on('connection', function (connection) {
                    connection.on('message', function (message) {
                        connection.send('invalid json');
                    });
                });
                client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
                done();
            }).to.throw();
        }, 1000);
    });
    it('should stop trying to reconnect to the server', function (done) {
        var connections = 0;
        wsServer.on('connection', function (connection) {
            connections += 1;
            if (connections === 1) {
                wsServer.close();
            }
            else {
                chai_1.assert(false);
            }
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            timeout: 100,
            reconnect: true,
            reconnectionAttempts: 1,
        });
        setTimeout(function () {
            chai_1.expect(client.client.readyState).to.be.equal(client.client.CLOSED);
            done();
        }, 500);
    });
});
describe('Server', function () {
    var onSubscribeSpy;
    beforeEach(function () {
        onSubscribeSpy = sinon.spy(handlers, 'onSubscribe');
    });
    afterEach(function () {
        if (onSubscribeSpy) {
            onSubscribeSpy.restore();
        }
        if (eventsOptions) {
            eventsOptions.onConnect.reset();
            eventsOptions.onDisconnect.reset();
            eventsOptions.onSubscribe.reset();
            eventsOptions.onUnsubscribe.reset();
        }
    });
    it('should throw an exception when creating a server without subscriptionManager', function () {
        chai_1.expect(function () {
            new server_1.SubscriptionServer({ subscriptionManager: undefined }, { server: httpServer });
        }).to.throw();
    });
    it('should trigger onConnect when client connects and validated', function (done) {
        new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        setTimeout(function () {
            chai_1.assert(eventsOptions.onConnect.calledOnce);
            done();
        }, 200);
    });
    it('should trigger onConnect with the correct connectionParams', function (done) {
        var connectionParams = {
            test: true,
        };
        new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/", {
            connectionParams: connectionParams,
        });
        setTimeout(function () {
            chai_1.assert(eventsOptions.onConnect.calledOnce);
            chai_1.expect(JSON.stringify(eventsOptions.onConnect.getCall(0).args[0])).to.equal(JSON.stringify(connectionParams));
            done();
        }, 200);
    });
    it('should trigger onConnect and return init_fail with error', function (done) {
        var connectionCallbackSpy = sinon.spy();
        new client_1.SubscriptionClient("ws://localhost:" + ONCONNECT_ERROR_TEST_PORT + "/", {
            connectionCallback: connectionCallbackSpy,
        });
        setTimeout(function () {
            chai_1.expect(connectionCallbackSpy.calledOnce).to.be.true;
            chai_1.expect(connectionCallbackSpy.getCall(0).args[0]).to.equal('Error');
            done();
        }, 200);
    });
    it('should trigger onDisconnect when client disconnects', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        client.client.close();
        setTimeout(function () {
            chai_1.assert(eventsOptions.onDisconnect.calledOnce);
            done();
        }, 200);
    });
    it('should call unsubscribe when client closes the connection', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        var spy = sinon.spy(eventsServer, 'unsubscribe');
        client.subscribe({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }, function (error, result) {
        });
        setTimeout(function () {
            client.client.close();
        }, 500);
        setTimeout(function () {
            chai_1.assert(spy.calledOnce);
            done();
        }, 1000);
    });
    it('should trigger onSubscribe when client subscribes', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        client.subscribe({
            query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }, function (error, result) {
            if (error) {
                chai_1.assert(false);
            }
        });
        setTimeout(function () {
            chai_1.assert(eventsOptions.onSubscribe.calledOnce);
            done();
        }, 200);
    });
    it('should trigger onUnsubscribe when client unsubscribes', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        var subId = client.subscribe({
            query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }, function (error, result) {
        });
        client.unsubscribe(subId);
        setTimeout(function () {
            chai_1.assert(eventsOptions.onUnsubscribe.calledOnce);
            done();
        }, 200);
    });
    it('should send correct results to multiple clients with subscriptions', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var client1 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var numResults = 0;
        setTimeout(function () {
            client.subscribe({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
                if (error) {
                    chai_1.assert(false);
                }
                if (result) {
                    chai_1.assert.property(result, 'user');
                    chai_1.assert.equal(result.user.id, '3');
                    chai_1.assert.equal(result.user.name, 'Jessie');
                    numResults++;
                }
                else {
                }
            });
        }, 100);
        var client11 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var numResults1 = 0;
        setTimeout(function () {
            client11.subscribe({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: 2,
                },
            }, function (error, result) {
                if (error) {
                    chai_1.assert(false);
                }
                if (result) {
                    chai_1.assert.property(result, 'user');
                    chai_1.assert.equal(result.user.id, '2');
                    chai_1.assert.equal(result.user.name, 'Marie');
                    numResults1++;
                }
            });
        }, 100);
        setTimeout(function () {
            subscriptionManager.publish('user', {});
        }, 200);
        setTimeout(function () {
            client.unsubscribeAll();
            chai_1.expect(numResults).to.equals(1);
            client1.unsubscribeAll();
            chai_1.expect(numResults1).to.equals(1);
            done();
        }, 300);
    });
    it('should send a subscription_fail message to client with invalid query', function (done) {
        var client1 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client1.client.onmessage = function (message) {
                var messageData = JSON.parse(message.data);
                chai_1.assert.equal(messageData.type, messageTypes_1.SUBSCRIPTION_FAIL);
                chai_1.assert.isAbove(messageData.payload.errors.length, 0, 'Number of errors is greater than 0.');
                done();
            };
            client1.subscribe({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            birthday\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
            });
        }, 100);
    });
    it('should set up the proper filters when subscribing', function (done) {
        var numTriggers = 0;
        var client3 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var client4 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client3.subscribe({
                query: "subscription userInfoFilter1($id: String) {\n            userFiltered(id: $id) {\n              id\n              name\n            }\n          }",
                operationName: 'userInfoFilter1',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
                if (error) {
                    chai_1.assert(false);
                }
                if (result) {
                    numTriggers += 1;
                    chai_1.assert.property(result, 'userFiltered');
                    chai_1.assert.equal(result.userFiltered.id, '3');
                    chai_1.assert.equal(result.userFiltered.name, 'Jessie');
                }
            });
            client4.subscribe({
                query: "subscription userInfoFilter1($id: String) {\n            userFiltered(id: $id) {\n              id\n              name\n            }\n          }",
                operationName: 'userInfoFilter1',
                variables: {
                    id: 1,
                },
            }, function (error, result) {
                if (result) {
                    numTriggers += 1;
                    chai_1.assert.property(result, 'userFiltered');
                    chai_1.assert.equal(result.userFiltered.id, '1');
                    chai_1.assert.equal(result.userFiltered.name, 'Dan');
                }
                if (error) {
                    chai_1.assert(false);
                }
            });
        }, 100);
        setTimeout(function () {
            subscriptionManager.publish('userFiltered', { id: 1 });
            subscriptionManager.publish('userFiltered', { id: 2 });
            subscriptionManager.publish('userFiltered', { id: 3 });
        }, 200);
        setTimeout(function () {
            chai_1.assert.equal(numTriggers, 2);
            done();
        }, 300);
    });
    it('correctly sets the context in onSubscribe', function (done) {
        var CTX = 'testContext';
        var client3 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client3.subscribe({
            query: "subscription context {\n          context\n        }",
            variables: {},
            context: CTX,
        }, function (error, result) {
            client3.unsubscribeAll();
            if (error) {
                chai_1.assert(false);
            }
            if (result) {
                chai_1.assert.property(result, 'context');
                chai_1.assert.equal(result.context, CTX);
            }
            done();
        });
        setTimeout(function () {
            subscriptionManager.publish('context', {});
        }, 100);
    });
    it('passes through webSocketRequest to onSubscribe', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.subscribe({
            query: "\n        subscription context {\n          context\n        }\n      ",
            variables: {},
        }, function (error, result) {
            if (error) {
                chai_1.assert(false);
            }
        });
        setTimeout(function () {
            chai_1.assert(onSubscribeSpy.calledOnce);
            chai_1.expect(onSubscribeSpy.getCall(0).args[2]).to.not.be.undefined;
            done();
        }, 100);
    });
    it('does not send more subscription data after client unsubscribes', function () {
        var client4 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            var subId = client4.subscribe({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }, function (error, result) {
            });
            client4.unsubscribe(subId);
        }, 100);
        setTimeout(function () {
            subscriptionManager.publish('user', {});
        }, 200);
        client4.client.onmessage = function (message) {
            if (JSON.parse(message.data).type === messageTypes_1.SUBSCRIPTION_DATA) {
                chai_1.assert(false);
            }
        };
    });
    it('rejects a client that does not specify a supported protocol', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/");
        client.on('close', function (code) {
            if (code === 1002) {
                done();
            }
            else {
                chai_1.assert(false);
            }
        });
    });
    it('rejects unparsable message', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/", protocols_1.GRAPHQL_SUBSCRIPTIONS);
        client.onmessage = function (message) {
            var messageData = JSON.parse(message.data);
            chai_1.assert.equal(messageData.type, messageTypes_1.SUBSCRIPTION_FAIL);
            chai_1.assert.isAbove(messageData.payload.errors.length, 0, 'Number of errors is greater than 0.');
            client.close();
            done();
        };
        client.onopen = function () {
            client.send('HI');
        };
    });
    it('rejects nonsense message', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/", protocols_1.GRAPHQL_SUBSCRIPTIONS);
        client.onmessage = function (message) {
            var messageData = JSON.parse(message.data);
            chai_1.assert.equal(messageData.type, messageTypes_1.SUBSCRIPTION_FAIL);
            chai_1.assert.isAbove(messageData.payload.errors.length, 0, 'Number of errors is greater than 0.');
            client.close();
            done();
        };
        client.onopen = function () {
            client.send(JSON.stringify({}));
        };
    });
    it('does not crash on unsub for Object.prototype member', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/", protocols_1.GRAPHQL_SUBSCRIPTIONS);
        client.onopen = function () {
            client.send(JSON.stringify({ type: messageTypes_1.SUBSCRIPTION_END, id: 'toString' }));
            setTimeout(done, 10);
        };
    });
    it('sends back any type of error', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.subscribe({
            query: "invalid useInfo{\n          error\n        }",
            variables: {},
        }, function (errors, result) {
            client.unsubscribeAll();
            chai_1.assert.isAbove(errors.length, 0, 'Number of errors is greater than 0.');
            done();
        });
    });
    it('handles errors prior to graphql execution', function (done) {
        handlers.onSubscribe = function (msg, params, webSocketRequest) {
            return Promise.resolve(Object.assign({}, params, {
                context: function () {
                    throw new Error('bad');
                },
            }));
        };
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.subscribe({
            query: "\n        subscription context {\n          context\n        }\n      ",
            variables: {},
            context: {},
        }, function (error, result) {
            client.unsubscribeAll();
            if (error) {
                chai_1.assert(Array.isArray(error));
                chai_1.assert.equal(error[0].message, 'bad');
            }
            else {
                chai_1.assert(false);
            }
            done();
        });
        setTimeout(function () {
            subscriptionManager.publish('context', {});
        }, 100);
    });
    it('sends a keep alive signal in the socket', function (done) {
        var client = new WebSocket("ws://localhost:" + KEEP_ALIVE_TEST_PORT + "/", protocols_1.GRAPHQL_SUBSCRIPTIONS);
        var yieldCount = 0;
        client.onmessage = function (message) {
            var parsedMessage = JSON.parse(message.data);
            if (parsedMessage.type === messageTypes_1.KEEPALIVE) {
                yieldCount += 1;
                if (yieldCount > 1) {
                    client.close();
                    done();
                }
            }
        };
    });
});
//# sourceMappingURL=tests.js.map
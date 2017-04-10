import * as WebSocket from 'ws';

import {
  SUBSCRIPTION_FAIL,
  SUBSCRIPTION_DATA,
  SUBSCRIPTION_START,
  SUBSCRIPTION_END,
  SUBSCRIPTION_SUCCESS,
  KEEPALIVE,
  INIT,
  INIT_FAIL,
  INIT_SUCCESS,
} from './messageTypes';
import {GRAPHQL_SUBSCRIPTIONS} from './protocols';
import {SubscriptionManager} from 'graphql-subscriptions';
import isObject = require('lodash.isobject');

type ConnectionSubscriptions = {[subId: string]: number};

type ConnectionContext = {
  initPromise?: Promise<any>
};

export interface SubscribeMessage {
  [key: string]: any; // any extension that will come with the message.
  payload: string;
  query?: string;
  variables?: {[key: string]: any};
  operationName?: string;
  id: string;
  type: string;
}

export interface ServerOptions {
  subscriptionManager: SubscriptionManager;
  onSubscribe?: Function;
  onUnsubscribe?: Function;
  onConnect?: Function;
  onDisconnect?: Function;
  keepAlive?: number;
  jsonReviver?: (key: any, value: any) => any;
  // contextValue?: any;
  // rootValue?: any;
  // formatResponse?: (Object) => Object;
  // validationRules?: Array<any>;
  // triggerGenerator?: (name: string, args: Object, context?: Object) => Array<{name: string, filter: Function}>;
}

export class SubscriptionServer {
  private onSubscribe: Function;
  private onUnsubscribe: Function;
  private onConnect: Function;
  private onDisconnect: Function;
  private wsServer: WebSocket.Server;
  private subscriptionManager: SubscriptionManager;
  private jsonReviver?: (key: any, value: any) => any;

  constructor(options: ServerOptions, socketOptions: WebSocket.IServerOptions) {
    const {subscriptionManager, onSubscribe, onUnsubscribe, onConnect, onDisconnect, keepAlive, jsonReviver} = options;

    if (!subscriptionManager) {
      throw new Error('Must provide `subscriptionManager` to websocket server constructor.');
    }

    this.subscriptionManager = subscriptionManager;
    this.onSubscribe = onSubscribe;
    this.onUnsubscribe = onUnsubscribe;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
    this.jsonReviver = jsonReviver;

    // init and connect websocket server to http
    this.wsServer = new WebSocket.Server(socketOptions || {});

    this.wsServer.on('connection', (request: WebSocket) => {
      if (request.protocol === undefined || request.protocol.indexOf(GRAPHQL_SUBSCRIPTIONS) === -1) {
        // Close the connection with an error code, and
        // then terminates the actual network connection (sends FIN packet)
        // 1002: protocol error
        request.close(1002);
        request.terminate();

        return;
      }

      // Regular keep alive messages if keepAlive is set
      if (keepAlive) {
        const keepAliveTimer = setInterval(() => {
          if (request.readyState === WebSocket.OPEN) {
            this.sendKeepAlive(request);
          } else {
            clearInterval(keepAliveTimer);
          }
        }, keepAlive);
      }

      const connectionSubscriptions: ConnectionSubscriptions = Object.create(null);
      const connectionContext: ConnectionContext = Object.create(null);

      request.on('message', this.onMessage(request, connectionSubscriptions, connectionContext));
      request.on('close', () => {
        this.onClose(request, connectionSubscriptions)();

        if (this.onDisconnect) {
          this.onDisconnect(request);
        }
      });
    });
  }

  private unsubscribe(connection: WebSocket, handleId: number) {
    this.subscriptionManager.unsubscribe(handleId);

    if (this.onUnsubscribe) {
      this.onUnsubscribe(connection);
    }
  }

  private onClose(connection: WebSocket, connectionSubscriptions: ConnectionSubscriptions) {
    return () => {
      Object.keys(connectionSubscriptions).forEach((subId) => {
        this.unsubscribe(connection, connectionSubscriptions[subId]);
        delete connectionSubscriptions[subId];
      });
    };
  }

  private onMessage(connection: WebSocket, connectionSubscriptions: ConnectionSubscriptions, connectionContext: ConnectionContext) {
    let onInitResolve: any = null, onInitReject: any = null;

    connectionContext.initPromise = new Promise((resolve, reject) => {
      onInitResolve = resolve;
      onInitReject = reject;
    });

    return (message: any) => {
      let parsedMessage: SubscribeMessage;
      try {
        parsedMessage = JSON.parse(message, this.jsonReviver);
      } catch (e) {
        this.sendSubscriptionFail(connection, null, {errors: [{message: e.message}]});
        return;
      }

      const subId = parsedMessage.id;
      switch (parsedMessage.type) {
        case INIT:
          let onConnectPromise = Promise.resolve(true);
          if (this.onConnect) {
            onConnectPromise = new Promise((resolve, reject) => {
              try {
                resolve(this.onConnect(parsedMessage.payload, connection));
            } catch (e) {
                reject(e);
              }
            });
          }

          onInitResolve(onConnectPromise);

          connectionContext.initPromise.then((result) => {
            if (result === false) {
              throw new Error('Prohibited connection!');
            }

            return {
              type: INIT_SUCCESS,
            };
          }).catch((error: Error) => {
            return {
              type: INIT_FAIL,
              payload: {
                error: error.message,
              },
            };
          }).then((resultMessage: any) => {
            this.sendInitResult(connection, resultMessage);
          });

          break;

        case SUBSCRIPTION_START:
          connectionContext.initPromise.then((initResult) => {
            const baseParams = {
              query: parsedMessage.query,
              variables: parsedMessage.variables,
              operationName: parsedMessage.operationName,
              context: Object.assign({}, isObject(initResult) ? initResult : {}),
              formatResponse: <any>undefined,
              formatError: <any>undefined,
              callback: <any>undefined,
            };
            let promisedParams = Promise.resolve(baseParams);

            if (this.onSubscribe) {
              promisedParams = Promise.resolve(this.onSubscribe(parsedMessage, baseParams, connection));
            }

            // if we already have a subscription with this id, unsubscribe from it first
            // TODO: test that this actually works
            if (connectionSubscriptions[subId]) {
              this.unsubscribe(connection, connectionSubscriptions[subId]);
              delete connectionSubscriptions[subId];
            }

            promisedParams.then(params => {
              if (typeof params !== 'object') {
                const error = `Invalid params returned from onSubscribe! return values must be an object!`;
                this.sendSubscriptionFail(connection, subId, {
                  errors: [{
                    message: error,
                  }],
                });

                throw new Error(error);
              }

              // create a callback
              // error could be a runtime exception or an object with errors
              // result is a GraphQL ExecutionResult, which has an optional errors property
              params.callback = (error: any, result: any) => {
                if (!error) {
                  this.sendSubscriptionData(connection, subId, result);
                } else if (error.errors) {
                  this.sendSubscriptionData(connection, subId, {errors: error.errors});
                } else {
                  this.sendSubscriptionData(connection, subId, {errors: [{message: error.message}]});
                }
              };

              return this.subscriptionManager.subscribe(params);
            }).then((graphqlSubId: number) => {
              connectionSubscriptions[subId] = graphqlSubId;
              this.sendSubscriptionSuccess(connection, subId);
            }).catch(e => {
              if (e.errors) {
                this.sendSubscriptionFail(connection, subId, {errors: e.errors});
              } else {
                this.sendSubscriptionFail(connection, subId, {errors: [{message: e.message}]});
              }
              return;
            });
          });
          break;

        case SUBSCRIPTION_END:
          connectionContext.initPromise.then(() => {
            // find subscription id. Call unsubscribe.
            // TODO untested. catch errors, etc.
            if (typeof connectionSubscriptions[subId] !== 'undefined') {
              this.unsubscribe(connection, connectionSubscriptions[subId]);
              delete connectionSubscriptions[subId];
            }
          });
          break;

        default:
          this.sendSubscriptionFail(connection, subId, {
            errors: [{
              message: 'Invalid message type!',
            }],
          });
      }
    };
  }

  private sendSubscriptionData(connection: WebSocket, subId: string, payload: any): void {
    let message = {
      type: SUBSCRIPTION_DATA,
      id: subId,
      payload,
    };

    connection.send(JSON.stringify(message));
  }

  private sendSubscriptionFail(connection: WebSocket, subId: string, payload: any): void {
    let message = {
      type: SUBSCRIPTION_FAIL,
      id: subId,
      payload,
    };

    connection.send(JSON.stringify(message));
  }

  private sendSubscriptionSuccess(connection: WebSocket, subId: string): void {
    let message = {
      type: SUBSCRIPTION_SUCCESS,
      id: subId,
    };

    connection.send(JSON.stringify(message));
  }

  private sendInitResult(connection: WebSocket, result: any): void {
    connection.send(JSON.stringify(result), () => {
      if (result.type === INIT_FAIL) {
        // Close the connection with an error code, and
        // then terminates the actual network connection (sends FIN packet)
        // 1011: an unexpected condition prevented the request from being fulfilled
        // We are using setTimeout because we want the message to be flushed before
        // disconnecting the client
        setTimeout(() => {
          connection.close(1011);
          connection.terminate();
        }, 10);
      }
    });
  }

  private sendKeepAlive(connection: WebSocket): void {
    let message = {
      type: KEEPALIVE,
    };

    connection.send(JSON.stringify(message));
  }
}

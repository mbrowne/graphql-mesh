import { join } from 'path';
import { GraphQLSchema, printSchema, validateSchema } from 'graphql';
import lodashGet from 'lodash.get';
import InMemoryLRUCache from '@graphql-mesh/cache-localforage';
import { InMemoryStoreStorageAdapter, MeshStore } from '@graphql-mesh/store';
import type { KeyValueCache, YamlConfig } from '@graphql-mesh/types';
import { defaultImportFn, PubSub } from '@graphql-mesh/utils';
import { DefaultLogger } from '@graphql-mesh/utils';
import {
  loadPackageDefinition,
  Server,
  ServerCredentials,
  ServiceClientConstructor,
} from '@grpc/grpc-js';
import { load } from '@grpc/proto-loader';
import { fetch as fetchFn } from '@whatwg-node/fetch';
import GrpcHandler from '../../src/index.js';

const wrapServerWithReflection = require('grpc-node-server-reflection').default;

const TEST_GRPC_SERVER_PORT = process.env.TEST_GRPC_SERVER_PORT || 50051;

describe('gRPC Handler integration tests', () => {
  let cache: KeyValueCache;
  let pubsub: PubSub;
  let store: MeshStore;
  let logger: DefaultLogger;
  beforeEach(() => {
    cache = new InMemoryLRUCache();
    pubsub = new PubSub();
    store = new MeshStore('grpc-test', new InMemoryStoreStorageAdapter(), {
      readonly: false,
      validate: false,
    });
    logger = new DefaultLogger('grpc-test');
  });
  afterEach(() => {
    pubsub.publish('destroy', undefined);
  });

  test('Load protobuf via reflection', async () => {
    // load protobuf from two different packages
    const server = await startGrpcServer({
      'comments.proto': { servicePath: 'foo.SampleService' },
      'empty.proto': { servicePath: 'io.xtech.Example' },
    });

    const config: YamlConfig.GrpcHandler = {
      endpoint: 'localhost:' + TEST_GRPC_SERVER_PORT,
    };

    const handler = new GrpcHandler({
      name: Date.now().toString(),
      config,
      cache,
      pubsub,
      store,
      logger,
      importFn: defaultImportFn,
      baseDir: __dirname,
    });

    const { schema } = await handler.getMeshSource({ fetchFn });

    expect(schema).toBeInstanceOf(GraphQLSchema);
    expect(validateSchema(schema)).toHaveLength(0);
    expect(printSchema(schema)).toMatchSnapshot();

    return new Promise<void>(resolve => {
      server.tryShutdown(err => {
        if (err) {
          server.forceShutdown();
        }
        resolve();
      });
    });
  });
});

type ProtoMap = {
  [protoFilePath: string]: {
    servicePath: string;
  };
};

async function startGrpcServer(protoMap: ProtoMap) {
  const server: Server = wrapServerWithReflection(new Server());

  for (const [protoFilePath, { servicePath }] of Object.entries(protoMap)) {
    const packageDefinition = await load(protoFilePath, {
      includeDirs: [join(__dirname, '../fixtures/proto-tests')],
    });
    const grpcObject = loadPackageDefinition(packageDefinition);
    server.addService((lodashGet(grpcObject, servicePath) as ServiceClientConstructor).service, {});
  }

  return new Promise<Server>((resolve, reject) => {
    server.bindAsync(
      'localhost:' + TEST_GRPC_SERVER_PORT,
      ServerCredentials.createInsecure(),
      (error, port) => {
        if (error) {
          reject(error);
          return;
        }
        server.start();

        console.log('gRPC Server started, listening: localhost:' + port);
        resolve(server);
      },
    );
  });
}

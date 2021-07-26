import {KafkaContainer, StartedKafkaContainer} from 'testcontainers';
import {Kafka, Producer, Consumer, Admin, logLevel} from 'kafkajs';
import {createStream} from '../src';

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('with real kafka', () => {
  jest.setTimeout(240_000);

  let container: StartedKafkaContainer;
  let consumer: Consumer;
  let producer: Producer;
  let admin: Admin;
  let kafka: Kafka;
  beforeAll(async () => {
    container = await new KafkaContainer().withExposedPorts(9093).start();
    kafka = new Kafka({
      logLevel: logLevel.NOTHING,
      brokers: [`${container.getHost()}:${container.getMappedPort(9093)}`],
    });
    producer = kafka.producer();
    await producer.connect();
    admin = kafka.admin();
    await admin.connect();
    consumer = kafka.consumer({groupId: 'output-consumer'});
    await consumer.connect();
  });
  afterAll(async () => {
    if (consumer) await consumer.disconnect();
    if (producer) await producer.disconnect();
    if (admin) await admin.disconnect();
    if (container) await container.stop();
  });

  /*it('identical stream', async () => {
    const inTopic = 'in-topic';
    const outTopic = 'out-topic';
    await consumer.subscribe({topic: outTopic, fromBeginning: true});
    const rdata: string[] = [];
    consumer.run({
      eachMessage: async ({message}) => {
        rdata.push(JSON.parse(message.value!.toString()));
      },
    });

    const brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`];

    //await sleep(5000);

    const stream = createStream({
      brokers,
      inputTopic: inTopic,
      groupId: 'stream',
      commitInterval: 1000,
    }).writeTo(outTopic);
    stream.start();

    await sleep(5000);

    await producer.send({
      topic: inTopic,
      messages: [...Array(10).keys()].map(i => ({
        value: JSON.stringify({data: i}),
      })),
    });

    await sleep(5000);
    await stream.stop();
    const expectedData = [
      {data: 0},
      {data: 1},
      {data: 2},
      {data: 3},
      {data: 4},
      {data: 5},
      {data: 6},
      {data: 7},
      {data: 8},
      {data: 9},
    ];
    expect(rdata).toStrictEqual(expectedData);
    const stats = stream.flushStatistics();
    expect(JSON.stringify(stats)).toBe(JSON.stringify({
       recvTotal: 10,
       sendTotal: 10,
    }));

  });*/

  it('union stream', async () => {
    const inTopic1 = 'in-topic1';
    const inTopic2 = 'in-topic2';
    const outTopic = 'out-topic';
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{topic: inTopic1}, {topic: inTopic2}, {topic: outTopic}],
    });

    const brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`];

    await producer.send({
      topic: inTopic1,
      messages: [...Array(1).keys()].map(() => ({
        value: '1',
      })),
    });
    await producer.send({
      topic: inTopic2,
      messages: [...Array(1).keys()].map(() => ({
        value: '2',
      })),
    });

    await consumer.subscribe({topic: outTopic, fromBeginning: true});
    const rdata: string[] = [];
    consumer.run({
      eachMessage: async ({message}) => {
        rdata.push(JSON.parse(message.value!.toString()));
      },
    });

    const stream1 = createStream<number>({
      brokers,
      inputTopic: inTopic1,
      groupId: 'stream1',
      commitInterval: 1000,
      fromBeginning: true,
    });
    const stream2 = createStream<number>({
      brokers,
      inputTopic: inTopic2,
      groupId: 'stream2',
      commitInterval: 1000,
      fromBeginning: true,
    }).map(t => t * 10);
    const stream = stream1.union(stream2).writeTo(outTopic);
    stream.start();

    await sleep(20_000);
    await stream.stop();
    const expectedData = [20, 1];
    expect(rdata.sort()).toEqual(expectedData.sort());
    const stats = stream.flushStatistics();
    expect(JSON.stringify(stats)).toBe(
      JSON.stringify({
        recvTotal: 2,
        sendTotal: 2,
      })
    );
  });
});

'use strict';

const assert = require('node:assert/strict');
const kafkajs = require('kafkajs');
const factory = require('../../lib/kafka-factory');

describe('lib/kafka-factory', function () {
    afterEach(function () {
        factory.setKafkajs(null);
    });

    it('returns the real kafkajs by default, with Snappy and LZ4 registered', function () {
        const k = factory.getKafkajs();
        assert.equal(k, kafkajs);
        assert.deepEqual(factory.getCodecSupport(), { snappy: true, lz4: true });
        const codec = k.CompressionCodecs[k.CompressionTypes.LZ4]();
        assert.equal(typeof codec.compress, 'function');
    });

    it('lets tests inject a fake and restore the real module with null', function () {
        const fake = { Kafka: class {} };
        factory.setKafkajs(fake);
        assert.equal(factory.getKafkajs(), fake);
        factory.setKafkajs(null);
        assert.equal(factory.getKafkajs(), kafkajs);
        factory.setKafkajs(undefined);
        assert.equal(factory.getKafkajs(), kafkajs);
    });

    it('returns a copy of the codec support flags', function () {
        const support = factory.getCodecSupport();
        support.snappy = false;
        assert.equal(factory.getCodecSupport().snappy, true);
    });
});

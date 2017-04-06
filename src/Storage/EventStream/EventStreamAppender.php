<?php

namespace Honeybee\CouchDb\Storage\EventStream;

use Honeybee\Common\Error\RuntimeError;
use Honeybee\CouchDb\Storage\CouchDbStorage;
use Honeybee\Model\Event\AggregateRootEventInterface;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageWriterInterface;

class EventStreamAppender extends CouchDbStorage implements StorageWriterInterface
{
    public function write($domainEvent, SettingsInterface $settings = null)
    {
        if (!$domainEvent instanceof AggregateRootEventInterface) {
            throw new RuntimeError(
                sprintf(
                    'Invalid payload given to %s, expected type of %s',
                    __METHOD__,
                    AggregateRootEventInterface::CLASS
                )
            );
        }

        $data = $domainEvent->toArray();
        $identifier = sprintf('%s-%s', $domainEvent->getAggregateRootIdentifier(), $domainEvent->getSeqNumber());
        $response = $this->request($identifier, self::METHOD_PUT, $data);
        $responseData = json_decode($response->getBody(), true);

        if (!isset($responseData['ok']) || !isset($responseData['rev'])) {
            throw new RuntimeError("Failed to write data.");
        }
    }

    public function delete($identifier, SettingsInterface $settings = null)
    {
        throw new RuntimeError("Deleting domain events from the stream is not allowed!");
    }
}

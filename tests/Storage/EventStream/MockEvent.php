<?php

namespace Honeybee\Tests\CouchDb\Storage\EventStream;

use Honeybee\Model\Event\AggregateRootEventInterface;

class MockEvent implements AggregateRootEventInterface
{
    public function getUuid()
    {
    }

    public function getTimestamp()
    {
    }

    public function getDateTime()
    {
    }

    public function getIsoDate()
    {
    }

    public function getMetadata()
    {
    }

    public function getType()
    {
    }

    public function getAggregateRootIdentifier()
    {
    }

    public function getAggregateRootType()
    {
    }

    public function getData()
    {
    }

    public function getEmbeddedEntityEvents()
    {
    }

    public function getAffectedAttributeNames()
    {
    }

    public function getSeqNumber()
    {
    }
}

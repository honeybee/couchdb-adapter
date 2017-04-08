<?php

namespace Honeybee\Tests\CouchDb\Storage\EventStream;

use GuzzleHttp\Client;
use Honeybee\CouchDb\Connector\CouchDbConnector;
use Honeybee\CouchDb\Storage\EventStream\EventStreamAppender;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Model\Event\AggregateRootEventInterface;
use Honeybee\Tests\CouchDb\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class EventStreamAppenderTest extends TestCase
{
    private $mockClient;

    private $mockConnector;

    private $mockResponse;

    public function setUp()
    {
        $this->mockClient = Mockery::mock(Client::CLASS);
        $this->mockConnector = Mockery::mock(CouchDbConnector::CLASS);
        $this->mockConnector->shouldReceive('isConnected')->never();
        $this->mockResponse = Mockery::mock(Response::CLASS);
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testDelete()
    {
        $streamAppender = new EventStreamAppender($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $streamAppender->delete('');
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testWriteInvalidDomainEvent()
    {
        $streamAppender = new EventStreamAppender($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $streamAppender->write('');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testWriteErrorResponse()
    {
        $mockDomainevent = Mockery::mock(AggregateRootEventInterface::CLASS);
        $mockDomainevent->shouldReceive('toArray')->once()->withNoArgs()->andReturn([]);
        $mockDomainevent->shouldReceive('getAggregateRootIdentifier')->once()->withNoArgs()->andReturn('test_id');
        $mockDomainevent->shouldReceive('getSeqNumber')->once()->withNoArgs()->andReturn(2);
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn('{"error": "result"}');
        $this->mockClient->shouldReceive('send')->once()->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $streamAppender = new EventStreamAppender(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );
        $streamAppender->write($mockDomainevent);
    }

    public function testWrite()
    {
        $mockDomainevent = Mockery::mock(AggregateRootEventInterface::CLASS);
        $mockDomainevent->shouldReceive('toArray')->once()->withNoArgs()->andReturn([]);
        $mockDomainevent->shouldReceive('getAggregateRootIdentifier')->once()->withNoArgs()->andReturn('test_id');
        $mockDomainevent->shouldReceive('getSeqNumber')->once()->withNoArgs()->andReturn(2);
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn('{"ok": "yup", "rev": 1}');
        $this->mockClient->shouldReceive('send')->once()->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $streamAppender = new EventStreamAppender(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $this->assertNull($streamAppender->write($mockDomainevent));
    }
}

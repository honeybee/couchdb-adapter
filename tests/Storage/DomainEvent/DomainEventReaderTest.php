<?php

namespace Honeybee\Tests\CouchDb\Storage\DomainEvent;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Honeybee\CouchDb\Connector\CouchDbConnector;
use Honeybee\CouchDb\Storage\DomainEvent\DomainEventReader;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIteratorInterface;
use Honeybee\Tests\CouchDb\TestCase;
use Mockery;
use Psr\Log\NullLogger;
use Psr\Http\Message\RequestInterface;

class DomainEventReaderTest extends TestCase
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

    public function testGetIterator()
    {
        $stubbedEventReader = Mockery::mock(
            DomainEventReader::CLASS.'[readAll]',
            [$this->mockConnector, new ArrayConfig([]), new NullLogger]
        );
        $stubbedEventReader->shouldReceive('readAll')->once()->andReturn([]);
        $this->assertInstanceOf(StorageReaderIteratorInterface::CLASS, $stubbedEventReader->getIterator());
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadEmptyIdentifier()
    {
        $eventReader = new DomainEventReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read('');
    } //@codeCoverageIgnore

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadNonStringIdentifier()
    {
        $eventReader = new DomainEventReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read(['test_id']);
    } //@codeCoverageIgnore

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadMissingConfig()
    {
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new DomainEventReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read('test_id');
    } //@codeCoverageIgnore

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadRequestBuildingException()
    {
        $mockException = Mockery::mock(RequestException::CLASS);
        $this->mockConnector->shouldReceive('getConnection')->andThrow($mockException);
        $eventReader = new DomainEventReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read('test_id');
    } //@codeCoverageIgnore

    /**
     * @expectedException GuzzleHttp\Exception\RequestException
     */
    public function testReadRequestException()
    {
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getStatusCode')->once()->withNoArgs()->andReturn(401);
        $mockException = Mockery::mock(RequestException::CLASS);
        $mockException->shouldReceive('getResponse')->once()->withNoArgs()->andReturn($this->mockResponse);
        $this->mockClient->shouldReceive('send')->andThrow($mockException);

        $eventReader = new DomainEventReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );
        $eventReader->read('test_id');
    } //@codeCoverageIgnore

    public function testReadRequestException404()
    {
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getStatusCode')->once()->withNoArgs()->andReturn(404);
        $mockException = Mockery::mock(RequestException::CLASS);
        $mockException->shouldReceive('getResponse')->once()->withNoArgs()->andReturn($this->mockResponse);
        $this->mockClient->shouldReceive('send')->andThrow($mockException);

        $eventReader = new DomainEventReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $this->assertNull($eventReader->read('test_id'));
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadInvalidResponse()
    {
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn('{"bad": "result"}');
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $expectedRequest = new Request('get', '/test_db/test_id', ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new DomainEventReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $eventReader->read('test_id');
    } //@codeCoverageIgnore

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadMissingObjectType()
    {
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()
            ->andReturn(new ArrayConfig(['database' => 'test_db']));
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn('{"doc": []}');
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $expectedRequest = new Request('get', '/test_db/test_id', ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new DomainEventReader($this->mockConnector, new ArrayConfig([]), new NullLogger);

        $eventReader->read('test_id');
    } //@codeCoverageIgnore

    public function testRead()
    {
        $responseData = [
            'doc' => ['@type' => '\stdClass', 'key' => 'value']
        ];
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn(json_encode($responseData));
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $expectedRequest = new Request('get', '/test_db/test_id', ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new DomainEventReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $this->assertEquals(new \stdClass($responseData), $eventReader->read('test_id'));
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadAllNoDesignDoc()
    {
        $eventReader = new DomainEventReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->readAll();
    } //@codeCoverageIgnore

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadAllInvalidResponse()
    {
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn('{"bad": "response"}');
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $expectedRequest = new Request(
                    'get',
                    '/test_db/_design/test_design/_view/events_by_timestamp?include_docs=true&reduce=false&limit=100',
                    ['Accept' => 'application/json']
                );
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new DomainEventReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );
        $eventReader->readAll();
    } //@codeCoverageIgnore

    public function testReadAllUsingLastKey()
    {
        $responseData = [
            ['doc' => ['@type' => '\stdClass', 'iso_date' => 1]],
            ['doc' => ['@type' => '\stdClass', 'iso_date' => 2]]
        ];
        $this->mockConnector->shouldReceive('getConnection')->twice()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getBody')->twice()->andReturnValues([
            json_encode(['rows' => [$responseData[0]]]),
            json_encode(['rows' => [$responseData[1]]])
        ]);
        $counter = 0;
        $this->mockClient->shouldReceive('send')->twice()->with(Mockery::on(
            function (RequestInterface $request) use (&$counter) {
                $path = '/test_db/_design/test_design/_view/events_by_timestamp';
                $query = '?include_docs=true&reduce=false&limit=1';
                $query .= $counter ? '&skip=1&startkey=%221%22' : '';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                $counter++;
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new DomainEventReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design', 'limit' => 1]),
            new NullLogger
        );

        $this->assertEquals(
            [new \stdClass($responseData[0]['doc'])],
            $eventReader->readAll()
        );
        $this->assertEquals(
            [new \stdClass($responseData[1]['doc'])],
            $eventReader->readAll(new Settings(['first' => false]))
        );
    }
}

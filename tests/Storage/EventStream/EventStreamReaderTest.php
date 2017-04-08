<?php

namespace Honeybee\Tests\CouchDb\Storage\EventStream;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use Honeybee\CouchDb\Connector\CouchDbConnector;
use Honeybee\CouchDb\Storage\EventStream\EventStreamReader;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;
use Honeybee\Model\Event\AggregateRootEventList;
use Honeybee\Model\Event\EventStream;
use Honeybee\Tests\CouchDb\TestCase;
use Mockery;
use Psr\Log\NullLogger;
use Psr\Http\Message\RequestInterface;
use GuzzleHttp\Psr7\Request;
use Honeybee\Infrastructure\Config\Settings;

class EventStreamReaderTest extends TestCase
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
            EventStreamReader::CLASS.'[readAll]',
            [$this->mockConnector, new ArrayConfig([]), new NullLogger]
        );
        $stubbedEventReader->shouldReceive('readAll')->once()->andReturn([]);
        $this->assertInstanceOf(StorageReaderIterator::CLASS, $stubbedEventReader->getIterator());
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadEmptyIdentifier()
    {
        $eventReader = new EventStreamReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read('');
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadNonStringIdentifier()
    {
        $eventReader = new EventStreamReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read(['test_id']);
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadMissingDesignDoc()
    {
        $eventReader = new EventStreamReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventReader->read('test_id');
    }

    /**
     * @expectedException GuzzleHttp\Exception\RequestException
     */
    public function testReadRequestException()
    {
        $mockException = Mockery::mock(RequestException::CLASS);
        $mockException->shouldReceive('getResponse')->once()->withNoArgs()->andReturn($this->mockResponse);
        $this->mockResponse->shouldReceive('getStatusCode')->once()->withNoArgs()->andReturn(123);
        $this->mockClient->shouldReceive('send')->once()->andThrow($mockException);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $eventReader->read('test_id');
    }

    public function testReadRequestException404()
    {
        $mockException = Mockery::mock(RequestException::CLASS);
        $mockException->shouldReceive('getResponse')->once()->withNoArgs()->andReturn($this->mockResponse);
        $this->mockResponse->shouldReceive('getStatusCode')->once()->withNoArgs()->andReturn(404);
        $this->mockClient->shouldReceive('send')->once()->andThrow($mockException);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $this->assertNull($eventReader->read('test_id'));
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadInvalidResponse()
    {
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn('{"bad": "result"}');
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $path = '/test_db/_design/test_design/_view/event_stream';
                $query = '?startkey=%5B%22test_id%22%2C+%7B%7D%5D&endkey=%5B%22test_id%22%2C+1%5D'.
                    '&include_docs=true&reduce=false&descending=true&limit=1000';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $eventReader->read('test_id');
    }

    public function testReadEmpty()
    {
        $responseData = ['total_rows' => 0];
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn(json_encode($responseData));
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $path = '/test_db/_design/test_design/_view/event_stream';
                $query = '?startkey=%5B%22test_id%22%2C+%7B%7D%5D&endkey=%5B%22test_id%22%2C+1%5D'.
                    '&include_docs=true&reduce=false&descending=true&limit=1000';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $this->assertNull($eventReader->read('test_id'));
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testReadMissingObjectType()
    {
        $responseData = [
            'total_rows' => 1,
            'rows' => [['doc' => ['key' => 'value']]]
        ];
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn(json_encode($responseData));
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $path = '/test_db/_design/test_design/_view/event_stream';
                $query = '?startkey=%5B%22test_id%22%2C+%7B%7D%5D&endkey=%5B%22test_id%22%2C+1%5D'.
                    '&include_docs=true&reduce=false&descending=true&limit=1000';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $eventReader->read('test_id');
    }

    public function testRead()
    {
        $responseData = [
            'total_rows' => 1,
            'rows' => [['doc' => [
                '@type' => 'Honeybee\Tests\CouchDb\Storage\EventStream\MockEvent'
            ]]]
        ];
        $this->mockResponse->shouldReceive('getBody')->once()->andReturn(json_encode($responseData));
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $path = '/test_db/_design/test_design/_view/event_stream';
                $query = '?startkey=%5B%22test_id%22%2C+%7B%7D%5D&endkey=%5B%22test_id%22%2C+1%5D'.
                    '&include_docs=true&reduce=false&descending=true&limit=1000';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $expectedEventStream = new EventStream([
            'identifier' => 'test_id',
            'events' => new AggregateRootEventList([new MockEvent($responseData['rows'][0]['doc'])])
        ]);
        $this->assertEquals($expectedEventStream, $eventReader->read('test_id'));
    }

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
                $path = '/test_db/_design/default_views/_view/';
                $query = '?group=true&group_level=1&reduce=true';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );
        $eventReader->readAll();
    }

    public function testReadAll()
    {
        $streamResponseData = [
            'rows' => [
                ['doc' => [
                    '@type' => '\Honeybee\Tests\CouchDb\Storage\EventStream\MockEvent'],
                    'key' => ['key1'],
                    'value' => 1
                ],
                ['doc' => [
                    '@type' => '\Honeybee\Tests\CouchDb\Storage\EventStream\MockEvent'],
                    'key' => ['key2'],
                    'value' => 2
                ]
            ]
        ];
        $viewResponseData1 = [
            'total_rows' => 1,
            'rows' => [['doc' => [
                '@type' => 'Honeybee\Tests\CouchDb\Storage\EventStream\MockEvent',
                'key' => 'value1'
            ]]]
        ];
        $viewResponseData2 = [
            'total_rows' => 1,
            'rows' => [['doc' => [
                '@type' => 'Honeybee\Tests\CouchDb\Storage\EventStream\MockEvent',
                'key' => 'value2'
            ]]]
        ];
        $this->mockConnector->shouldReceive('getConnection')->times(3)->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->times(3)->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getBody')->times(3)->andReturnValues([
            json_encode($streamResponseData),
            json_encode($viewResponseData1),
            json_encode($viewResponseData2)
        ]);
        $counter = 0;
        $this->mockClient->shouldReceive('send')->times(3)->with(Mockery::on(
            function (RequestInterface $request) use (&$counter) {
                switch ($counter) {
                    case 0:
                        $path = '/test_db/_design/default_views/_view/';
                        $query = '?group=true&group_level=1&reduce=true';
                        break;
                    case 1:
                        $path = '/test_db/_design/test_design/_view/event_stream';
                        $query = '?startkey=%5B%22key1%22%2C+%7B%7D%5D&endkey=%5B%22key1%22%2C+1%5D'.
                            '&include_docs=true&reduce=false&descending=true&limit=1000';
                        break;
                    case 2:
                        $path = '/test_db/_design/test_design/_view/event_stream';
                        $query = '?startkey=%5B%22key2%22%2C+%7B%7D%5D&endkey=%5B%22key2%22%2C+1%5D'.
                            '&include_docs=true&reduce=false&descending=true&limit=1000';
                        break;
                }
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                $counter++;
                return true;
            }
        ))->andReturn($this->mockResponse);

        $eventReader = new EventStreamReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'design_doc' => 'test_design']),
            new NullLogger
        );

        $expectedEventStream1 = new EventStream([
            'identifier' => 'key1',
            'events' => new AggregateRootEventList([new MockEvent($viewResponseData1['rows'][0]['doc'])])
        ]);
        $this->assertEquals([$expectedEventStream1], $eventReader->readAll());

        $expectedEventStream2 = new EventStream([
            'identifier' => 'key2',
            'events' => new AggregateRootEventList([new MockEvent($viewResponseData2['rows'][0]['doc'])])
        ]);
        $this->assertEquals([$expectedEventStream2], $eventReader->readAll(new Settings(['first' => false])));
        $this->assertEquals([], $eventReader->readAll(new Settings(['first' => false])));
    }
}

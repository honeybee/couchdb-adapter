<?php

namespace Honeybee\Tests\CouchDb\Storage\StructureVersionList;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Honeybee\CouchDb\Connector\CouchDbConnector;
use Honeybee\CouchDb\Storage\StructureVersionList\StructureVersionListReader;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;
use Honeybee\Infrastructure\Migration\StructureVersion;
use Honeybee\Infrastructure\Migration\StructureVersionList;
use Honeybee\Tests\CouchDb\TestCase;
use Mockery;
use Psr\Log\NullLogger;
use Psr\Http\Message\RequestInterface;

class StructureVersionListReaderTest extends TestCase
{
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
        $versionListReader = Mockery::mock(
            StructureVersionListReader::CLASS.'[readAll]',
            [$this->mockConnector, new ArrayConfig([]), new NullLogger]
        );
        $versionListReader->shouldReceive('readAll')->once()->andReturn(['something']);
        $iterator = $versionListReader->getIterator();
        $this->assertInstanceOf(StorageReaderIterator::CLASS, $iterator);
        $this->assertTrue($iterator->valid());
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadEmptyIdentifier()
    {
        $versionListReader = new StructureVersionListReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $versionListReader->read('');
    } //@codeCoverageIgnore

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testReadNonStringIdentifier()
    {
        $versionListReader = new StructureVersionListReader($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $versionListReader->read(['test_id']);
    } //@codeCoverageIgnore

    public function testReadMissing404()
    {
        $mockException = Mockery::mock(RequestException::CLASS);
        $mockException->shouldReceive('getResponse')->once()->withNoArgs()->andReturnSelf();
        $mockException->shouldReceive('getStatusCode')->once()->withNoArgs()->andReturn(404);
        $this->mockClient->shouldReceive('send')->once()->andThrow($mockException);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $versionListReader = new StructureVersionListReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $this->assertNull($versionListReader->read('test_id'));
    }

    /**
     * @expectedException GuzzleHttp\Exception\RequestException
     */
    public function testReadRequestException()
    {
        $mockException = Mockery::mock(RequestException::CLASS);
        $mockException->shouldReceive('getResponse')->once()->withNoArgs()->andReturnSelf();
        $mockException->shouldReceive('getStatusCode')->once()->withNoArgs()->andReturn(123);
        $this->mockClient->shouldReceive('send')->once()->andThrow($mockException);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $versionListReader = new StructureVersionListReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $versionListReader->read('test_id');
    }

    public function testRead()
    {
        $responseData = ['_id' => 'test_id', '_rev' => 1, 'versions' => [['version' => 'data']]];
        $this->mockResponse->shouldReceive('getBody')->once()->withNoArgs()->andReturn(json_encode($responseData));
        $this->mockClient->shouldReceive('send')->once()->with(Mockery::on(
            function (RequestInterface $request) {
                $expectedRequest = new Request('get', '/test_db/test_id', ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $versionListReader = new StructureVersionListReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );

        $expectedVersionList = new StructureVersionList(
            'test_id',
            [new StructureVersion($responseData['versions'][0])]
        );
        $this->assertEquals($expectedVersionList, $versionListReader->read('test_id'));
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
                $expectedRequest = new Request(
                    'get',
                    '/test_db/_all_docs?include_docs=true&limit=10',
                    ['Accept' => 'application/json']
                );
                $this->assertEquals($expectedRequest, $request);
                return true;
            }
        ))->andReturn($this->mockResponse);

        $versionListReader= new StructureVersionListReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db']),
            new NullLogger
        );
        $versionListReader->readAll();
    } //@codeCoverageIgnore

    public function testReadAllUsingLastKey()
    {
        $responseData = [
            ['_id' => 'test_id1', 'versions' => [['version' => 'data1']]],
            ['_id' => 'test_id2', 'versions' => [['version' => 'data2']]]
        ];
        $this->mockConnector->shouldReceive('getConnection')->twice()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockResponse->shouldReceive('getBody')->twice()->andReturnValues([
            json_encode(['total_rows' => 2, 'offset' => 0, 'rows' => [$responseData[0]]]),
            json_encode(['total_rows' => 2, 'offset' => 1, 'rows' => [$responseData[1]]])
        ]);
        $counter = 0;
        $this->mockClient->shouldReceive('send')->twice()->with(Mockery::on(
            function (RequestInterface $request) use (&$counter) {
                $path = '/test_db/_all_docs';
                $query = '?include_docs=true&limit=5';
                $query .= $counter ? '&skip=1&startkey=%22test_id1%22' : '';
                $expectedRequest = new Request('get', $path.$query, ['Accept' => 'application/json']);
                $this->assertEquals($expectedRequest, $request);
                $counter++;
                return true;
            }
        ))->andReturn($this->mockResponse);

        $versionListReader= new StructureVersionListReader(
            $this->mockConnector,
            new ArrayConfig(['database' => 'test_db', 'limit' => 5]),
            new NullLogger
        );

        $this->assertEquals(
            [new StructureVersionList('test_id1', [new StructureVersion($responseData[0]['versions'][0])])],
            $versionListReader->readAll()
        );
        $this->assertEquals(
            [new StructureVersionList('test_id2', [new StructureVersion($responseData[1]['versions'][0])])],
            $versionListReader->readAll(new Settings(['first' => false]))
        );
        $this->assertEquals([], $versionListReader->readAll(new Settings(['first' => false])));
    }
}

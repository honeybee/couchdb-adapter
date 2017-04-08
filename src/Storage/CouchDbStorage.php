<?php

namespace Honeybee\CouchDb\Storage;

use Assert\Assertion;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Psr7\Request;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\Infrastructure\DataAccess\Storage\Storage;

abstract class CouchDbStorage extends Storage
{
    const METHOD_POST = 'post';

    const METHOD_PUT = 'put';

    const METHOD_GET = 'get';

    const METHOD_DELETE = 'delete';

    protected function request($identifier, $method, array $body = [], array $params = [])
    {
        $allowedMethods = [self::METHOD_GET, self::METHOD_POST, self::METHOD_PUT, self::METHOD_DELETE];
        if (!in_array($method, $allowedMethods)) {
            throw new RuntimeError(
                sprintf("Invalid method %s given. Expecting one of: %s", $method, implode(', ', $allowedMethods))
            );
        }

        if (isset($body['revision'])) {
            $params['rev'] = $body['revision'];
        }

        try {
            $client = $this->connector->getConnection();
            $requestPath = $this->buildRequestUrl($identifier, $params);
            if (empty($body)) {
                $request = new Request($method, $requestPath, ['Accept' => 'application/json']);
            } else {
                $request = new Request(
                    $method,
                    $requestPath,
                    ['Accept' => 'application/json', 'Content-Type' => 'application/json'],
                    json_encode($body)
                );
            }
        } catch (GuzzleException $guzzleError) {
            throw new RuntimeError(
                sprintf('Failed to build "%s" request: %s', $method, $guzzleError),
                0,
                $guzzleError
            );
        }

        return $client->send($request);
    }

    protected function buildRequestUrl($identifier, array $params = [])
    {
        $requestPath = '/' . $this->getDatabase() . '/' . $identifier;

        if (!empty($params)) {
            $requestPath .= '?' . http_build_query($params);
        }

        return str_replace('//', '/', $requestPath);
    }

    protected function getDatabase()
    {
        $fallbackDatabase = $this->connector->getConfig()->get('database');
        $database = $this->config->get('database', $fallbackDatabase);

        Assertion::string($database);
        Assertion::notBlank($database);

        return $database;
    }
}

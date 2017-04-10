<?php

namespace Honeybee\CouchDb\Storage\DomainEvent;

use Assert\Assertion;
use GuzzleHttp\Exception\RequestException;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\CouchDb\Storage\CouchDbStorage;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;

class DomainEventReader extends CouchDbStorage implements StorageReaderInterface
{
    protected $lastKey;

    public function read($identifier, SettingsInterface $settings = null)
    {
        Assertion::string($identifier);
        Assertion::notBlank($identifier);

        try {
            $path = sprintf('/%s', $identifier);
            $response = $this->request($path, self::METHOD_GET);
            $resultData = json_decode($response->getBody(), true);
        } catch (RequestException $error) {
            if ($error->getResponse()->getStatusCode() === 404) {
                return null;
            } else {
                throw $error;
            }
        }

        if (!isset($resultData['doc'])) {
            throw new RuntimeError(sprintf('Invalid doc response from CouchDB: %s', var_export($resultData, true)));
        }

        return $this->createDomainEvent($resultData['doc']);
    }

    public function readAll(SettingsInterface $settings = null)
    {
        $settings = $settings ?: new Settings;

        if ($settings->get('first', true)) {
            $this->lastKey = null;
        }

        $viewParams = [
            'include_docs' => 'true',
            'reduce' => 'false',
            'limit' => $this->config->get('limit', 100)
        ];

        if ($this->lastKey) {
            $viewParams['skip'] = 1;
            $viewParams['startkey'] = sprintf('"%s"', $this->lastKey);
        }

        if (!$this->config->has('design_doc')) {
            throw new RuntimeError(
                'Missing setting for "design_doc" that holds the name of the CouchDB design document, ' .
                'that is expected to contain the event_stream view.'
            );
        }

        $viewPath = sprintf(
            '/_design/%s/_view/%s',
            $this->config->get('design_doc'),
            $this->config->get('view_name', 'events_by_timestamp')
        );

        // @todo catch RequestException?
        $response = $this->request($viewPath, self::METHOD_GET, [], $viewParams);
        $resultData = json_decode($response->getBody(), true);

        if (!isset($resultData['rows'])) {
            throw new RuntimeError(sprintf('Invalid rows response from CouchDb: %s', var_export($resultData, true)));
        }

        $events = [];
        foreach ($resultData['rows'] as $eventData) {
            $events[] = $this->createDomainEvent($eventData['doc']);
            $this->lastKey = $eventData['doc']['iso_date'];
        }

        return $events;
    }

    public function getIterator()
    {
        return new StorageReaderIterator($this);
    }

    protected function createDomainEvent(array $eventData)
    {
        if (!isset($eventData[self::OBJECT_TYPE])) {
            throw new RuntimeError('Missing object type key within event data.');
        }

        return new $eventData[self::OBJECT_TYPE]($eventData);
    }
}

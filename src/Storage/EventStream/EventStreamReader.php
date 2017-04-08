<?php

namespace Honeybee\CouchDb\Storage\EventStream;

use Assert\Assertion;
use GuzzleHttp\Exception\RequestException;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\CouchDb\Storage\CouchDbStorage;
use Honeybee\Model\Event\AggregateRootEventList;
use Honeybee\Model\Event\EventStream;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;

class EventStreamReader extends CouchDbStorage implements StorageReaderInterface
{
    const STARTKEY_FILTER = '["%s", {}]';

    const ENDKEY_FILTER = '["%s", 1]';

    protected $nextIdentifier = null;

    protected $identifierList;

    public function read($identifier, SettingsInterface $settings = null)
    {
        Assertion::string($identifier);
        Assertion::notBlank($identifier);

        $viewParams = [
            'startkey' => sprintf(self::STARTKEY_FILTER, $identifier),
            'endkey' => sprintf(self::ENDKEY_FILTER, $identifier),
            'include_docs' => 'true',
            'reduce' => 'false',
            'descending' => 'true',
            'limit' => 1000 // @todo use snapshot size config setting as soon as available
        ];

        if (!$this->config->has('design_doc')) {
            throw new RuntimeError(
                'Missing setting for "design_doc" that holds the name of the couchdb design document, ' .
                'that is expected to contain the event_stream view.'
            );
        }

        $viewPath = sprintf(
            '/_design/%s/_view/%s',
            $this->config->get('design_doc'),
            $this->config->get('view_name', 'event_stream')
        );

        try {
            $response = $this->request($viewPath, self::METHOD_GET, [], $viewParams);
            $resultData = json_decode($response->getBody(), true);
        } catch (RequestException $error) {
            if ($error->getResponse()->getStatusCode() === 404) {
                return null;
            } else {
                throw $error;
            }
        }

        if (!isset($resultData['total_rows'])) {
            throw new RuntimeError(sprintf(
                'Invalid event_stream read response from CouchDB: %s',
                var_export($resultData, true)
            ));
        }

        if ($resultData['total_rows'] > 0) {
            return $this->createEventStream($identifier, array_reverse($resultData['rows']));
        }
    }

    public function readAll(SettingsInterface $settings = null)
    {
        $settings = $settings ?: new Settings;

        if ($settings->get('first', true)) {
            $this->identifierList = $this->fetchEventStreamIdentifiers();
        }
        $this->nextIdentifier = key($this->identifierList);
        next($this->identifierList);

        if (!$this->nextIdentifier) {
            return [];
        }

        return [$this->read($this->nextIdentifier, $settings)];
    }

    public function getIterator()
    {
        return new StorageReaderIterator($this);
    }

    protected function createEventStream($identifier, array $eventStreamData)
    {
        $events = [];
        foreach ($eventStreamData as $eventData) {
            $eventData = $eventData['doc'];
            if (!isset($eventData[self::OBJECT_TYPE])) {
                throw new RuntimeError('Missing type key within event data.');
            }
            $eventClass = $eventData[self::OBJECT_TYPE];
            $events[] = new $eventClass($eventData);
        }
        $data['identifier'] = $identifier;
        $data['events'] = new AggregateRootEventList($events);

        return new EventStream($data);
    }

    protected function fetchEventStreamIdentifiers()
    {
        $eventStreamKeys = [];
        $viewName = sprintf('/_design/default_views/_view/%s', $this->config->get('view_name'));

        $requestParams = [
            'group' => 'true',
            'group_level' => 1,
            'reduce' => 'true'
        ];

        $response = $this->request(
            sprintf('/_design/default_views/_view/%s', $this->config->get('view_name')),
            self::METHOD_GET,
            [],
            $requestParams
        );
        $resultData = json_decode($response->getBody(), true);

        if (!isset($resultData['rows'])) {
            throw new RuntimeError(sprintf('Invalid rows response from CouchDb: %s', var_export($resultData, true)));
        }

        foreach ($resultData['rows'] as $row) {
            $eventStreamKeys[$row['key'][0]] = $row['value'];
        }

        return $eventStreamKeys;
    }
}

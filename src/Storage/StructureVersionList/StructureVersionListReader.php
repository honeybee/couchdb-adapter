<?php

namespace Honeybee\CouchDb\Storage\StructureVersionList;

use Assert\Assertion;
use GuzzleHttp\Exception\RequestException;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\CouchDb\Storage\CouchDbStorage;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderInterface;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;
use Honeybee\Infrastructure\Migration\StructureVersionList;
use Honeybee\Infrastructure\Migration\StructureVersion;

class StructureVersionListReader extends CouchDbStorage implements StorageReaderInterface
{
    const READ_ALL_LIMIT = 10;

    protected $lastKey;

    public function read($identifier, SettingsInterface $settings = null)
    {
        Assertion::string($identifier);
        Assertion::notBlank($identifier);

        try {
            $response = $this->request($identifier, self::METHOD_GET);
            $resultData = json_decode($response->getBody(), true);
        } catch (RequestException $error) {
            if ($error->getResponse()->getStatusCode() === 404) {
                return null;
            } else {
                throw $error;
            }
        }

        $resultData['revision'] = $resultData['_rev'];

        return $this->createStructureVersionList($resultData['_id'], $resultData['versions']);
    }

    public function readAll(SettingsInterface $settings = null)
    {
        $settings = $settings ?: new Settings;

        $data = [];

        $defaultLimit = $this->config->get('limit', self::READ_ALL_LIMIT);
        $requestParams = [
            'include_docs' => 'true',
            'limit' => $settings->get('limit', $defaultLimit)
        ];

        if (!$settings->get('first', true)) {
            if (!$this->lastKey) {
                return $data;
            }

            $requestParams['skip'] = 1;
            $requestParams['startkey'] = sprintf('"%s"', $this->lastKey);
        }

        // @todo catch RequestException?
        $response = $this->request('_all_docs', self::METHOD_GET, [], $requestParams);
        $resultData = json_decode($response->getBody(), true);

        if (!isset($resultData['rows'])) {
            throw new RuntimeError(sprintf('Invalid rows response from CouchDb: %s', var_export($resultData, true)));
        }

        foreach ($resultData['rows'] as $row) {
            $data[] = $this->createStructureVersionList($row['_id'], $row['versions']);
        }

        if ($resultData['total_rows'] === $resultData['offset'] + 1) {
            $this->lastKey = null;
        } else {
            $lastRow = end($data);
            $this->lastKey = $lastRow->getIdentifier();
        }

        return $data;
    }

    public function getIterator()
    {
        return new StorageReaderIterator($this);
    }

    protected function createStructureVersionList($identifier, array $versions)
    {
        $structureVersions = [];
        foreach ($versions as $version) {
            $structureVersions[] = new StructureVersion($version);
        }

        return new StructureVersionList($identifier, $structureVersions);
    }
}

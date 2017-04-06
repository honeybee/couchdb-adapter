<?php

namespace Honeybee\CouchDb\Storage\StructureVersionList;

use GuzzleHttp\Exception\RequestException;
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

    protected $nextStartKey = null;

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
            if (!$this->nextStartKey) {
                return $data;
            }

            $requestParams['startkey'] = sprintf('"%s"', $this->nextStartKey);
            $requestParams['skip'] = 1;
        }

        $response = $this->request('_all_docs', self::METHOD_GET, [], $requestParams);
        $resultData = json_decode($response->getBody(), true);

        foreach ($resultData['rows'] as $row) {
            $data[] = $this->createStructureVersionList($row);
        }

        if ($resultData['total_rows'] === $resultData['offset'] + 1) {
            $this->nextStartKey = null;
        } else {
            $lastRow = end($data);
            $this->nextStartKey = $lastRow[self::DOMAIN_FIELD_ID];
        }

        return $data;
    }

    public function read($identifier, SettingsInterface $settings = null)
    {
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

        return $this->createStructureVersionList($resultData);
    }

    public function getIterator()
    {
        return new StorageReaderIterator($this);
    }

    protected function createStructureVersionList(array $data)
    {
        $structureVersionList = new StructureVersionList($data['_id']);

        foreach ($data['versions'] as $versionData) {
            $structureVersionList->push(new StructureVersion($versionData));
        }

        return $structureVersionList;
    }
}

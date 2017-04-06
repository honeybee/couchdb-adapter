<?php

namespace Honeybee\CouchDb\Storage\StructureVersionList;

use Assert\Assertion;
use GuzzleHttp\Exception\RequestException;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\CouchDb\Storage\CouchDbStorage;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageWriterInterface;
use Honeybee\Infrastructure\Migration\StructureVersionList;

class StructureVersionListWriter extends CouchDbStorage implements StorageWriterInterface
{
    public function write($structureVersionList, SettingsInterface $settings = null)
    {
        Assertion::isInstanceOf($structureVersionList, StructureVersionList::CLASS);

        $data = [
            'identifier' => $structureVersionList->getIdentifier(),
            'versions' => $structureVersionList->toArray()
        ];

        try {
            // @todo use head method to get current revision?
            $response = $this->request($data['identifier'], self::METHOD_GET);
            $structureVersion = json_decode($response->getBody(), true);
            $data['revision'] = $structureVersion['_rev'];
        } catch (RequestException $error) {
            error_log(__METHOD__ . ' - ' . $error->getMessage());
        }

        try {
            $response = $this->request($data['identifier'], self::METHOD_PUT, $data);
            $responseData = json_decode($response->getBody(), true);
        } catch (RequestException $error) {
            error_log(__METHOD__ . ' - ' . $error->getMessage());
        }

        if (!isset($responseData['ok']) || !isset($responseData['rev'])) {
            throw new RuntimeError('Failed to write data.');
        }
    }

    public function delete($identifier, SettingsInterface $settings = null)
    {
        try {
            $response = $this->request($identifier, self::METHOD_GET);
            $structureVersion = json_decode($response->getBody(), true);
            $data['revision'] = $structureVersion['_rev'];
            $this->request($identifier, self::METHOD_DELETE, [], $data);
        } catch (RequestException $error) {
            error_log(__METHOD__ . ' - ' . $error->getMessage());
        }
    }
}

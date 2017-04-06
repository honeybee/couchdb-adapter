<?php

namespace Honeybee\CouchDb\Migration;

use GuzzleHttp\Exception\RequestException;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\Infrastructure\Migration\Migration;
use Honeybee\Infrastructure\Migration\MigrationTargetInterface;

abstract class CouchDbMigration extends Migration
{
    const MAP_FILE_SUFFIX = '.map.js';

    const REDUCE_FILE_SUFFIX = '.reduce.js';

    abstract protected function getViewsDirectory();

    abstract protected function getDesignDocName();

    protected function createDatabaseIfNotExists(MigrationTargetInterface $migrationTarget, $updateViews = false)
    {
        if (!$this->databaseExists($migrationTarget)) {
            $this->createDatabase($migrationTarget, $updateViews);
        } elseif ($updateViews) {
            $this->updateDesignDoc($migrationTarget);
        }
    }

    protected function createDatabase(MigrationTargetInterface $migrationTarget, $updateViews = false)
    {
        try {
            $client = $this->getConnection($migrationTarget);
            $databaseName = $this->getDatabaseName($migrationTarget);
            $response = $client->put('/' . $databaseName);
            if ($response->getStatusCode() !== 201) {
                throw new RuntimeError(
                    'Failed to create couchdb database %s. Received status %s along with this data: %s',
                    $databaseName,
                    $response->getStatusCode(),
                    print_r(json_decode($response->getBody(), true), true)
                );
            }
        } catch (RequestException $error) {
            $errorData = json_decode($error->getResponse()->getBody(), true);
            throw new RuntimeError("Failed to create couchdb database. Reason: " . $errorData['reason']);
        }

        if ($updateViews) {
            $this->updateDesignDoc($migrationTarget);
        }
    }

    protected function deleteDatabase(MigrationTargetInterface $migrationTarget)
    {
        if ($this->databaseExists($migrationTarget)) {
            $client = $this->getConnection($migrationTarget);
            $databaseName = $this->getDatabaseName($migrationTarget);
            $response = $client->delete('/' . $databaseName);
            if ($response->getStatusCode() !== 200) {
                throw new RuntimeError(
                    'Failed to delete couchdb database %s. Received status %s along with this data: %s',
                    $databaseName,
                    $response->getStatusCode(),
                    print_r(json_decode($response->getBody(), true), true)
                );
            }
        }
    }

    protected function getDatabaseName(MigrationTargetInterface $migrationTarget)
    {
        $connector = $migrationTarget->getTargetConnector();
        $connectorConfig = $connector->getConfig();

        return $connectorConfig->get('database');
    }

    protected function updateDesignDoc(MigrationTargetInterface $migrationTarget)
    {
        $viewsDirectory = $this->getViewsDirectory();
        if (!is_dir($viewsDirectory)) {
            throw new RuntimeError(sprintf('Given views directory "%s" does not exist.', $viewsDirectory));
        }

        $views = [];
        $globExpression = sprintf('%s/*.map.js', $viewsDirectory);
        foreach (glob($globExpression) as $viewMapFile) {
            $reduceFunction = '';
            $mapFunction = file_get_contents($viewMapFile);
            $viewName = str_replace(self::MAP_FILE_SUFFIX, '', basename($viewMapFile));
            $views[$viewName] = ['map' => $mapFunction];

            $reduceFilePath = dirname($viewMapFile) . DIRECTORY_SEPARATOR . $viewName . self::REDUCE_FILE_SUFFIX;
            if (is_readable($reduceFilePath)) {
                $views[$viewName]['reduce'] = file_get_contents($reduceFilePath);
            }
        }

        $client = $this->getConnection($migrationTarget);
        $databaseName = $this->getDatabaseName($migrationTarget);
        $documentPath = sprintf('/%s/_design/%s', $databaseName, urlencode($this->getDesignDocName()));

        try {
            $response = $client->get($documentPath);
            $designDoc = json_decode($response->getBody(), true);
        } catch (RequestException $error) {
            $errorData = json_decode($error->getResponse()->getBody(), true);
            if ($errorData['error'] === 'not_found') {
                $designDoc = [];
            } else {
                throw $error;
            }
        }

        try {
            if (!empty($designDoc)) {
                $designDoc['views'] = $views;
                $payload = $designDoc;
            } else {
                $payload = ['language' => 'javascript', 'views' => $views];
            }
            $client->put($documentPath, ['body' => json_encode($payload)]);
        } catch (RequestException $error) {
            $errorData = json_decode($error->getResponse()->getBody(), true);
            throw new RuntimeError("Failed to create/update couchdb design-doc. Reason: " . $errorData['reason']);
        }
    }

    protected function deleteDesignDoc(MigrationTargetInterface $migrationTarget)
    {
        $client = $this->getConnection($migrationTarget);
        $databaseName = $this->getDatabaseName($migrationTarget);
        $documentPath = sprintf('/%s/_design/%s', $databaseName, urlencode($this->getDesignDocName()));

        try {
            $response = $client->get($documentPath);
            $curDocument = json_decode($response->getBody(), true);
            $client->delete(sprintf('%s?rev=%s', $documentPath, $curDocument['_rev']));
        } catch (RequestException $error) {
            $errorData = json_decode($error->getResponse()->getBody(), true);
            if ($errorData['error'] !== 'not_found') {
                throw new RuntimeError("Failed to delete couchdb design-doc. Reason: " . $errorData['reason']);
            }
        }
    }

    protected function databaseExists(MigrationTargetInterface $migrationTarget)
    {
        try {
            $databaseName = $this->getDatabaseName($migrationTarget);
            $client = $this->getConnection($migrationTarget);

            $response = $client->get('/' . $databaseName);
            return $response->getStatusCode() === 200;
        } catch (RequestException $error) {
            $errorData = json_decode($error->getResponse()->getBody(), true);
            if ($errorData['error'] === 'not_found') {
                return false;
            }
            throw $error;
        }
    }
}

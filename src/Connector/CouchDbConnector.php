<?php

namespace Honeybee\CouchDb\Connector;

use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Uri;
use GuzzleHttp\TransferStats;
use Honeybee\Infrastructure\DataAccess\Connector\Connector;
use Honeybee\Infrastructure\DataAccess\Connector\Status;
use Psr\Http\Message\RequestInterface;

class CouchDbConnector extends Connector
{
    /**
     * @return Client
     */
    protected function connect()
    {
        $baseUri = $this->config->get('base_uri');
        if ($this->config->has('transport') && $this->config->has('host') && $this->config->has('port')) {
            $baseUri = sprintf(
                '%s://%s:%s',
                $this->config->get('transport'),
                $this->config->get('host'),
                $this->config->get('port')
            );
        }

        $clientOptions = ['base_uri' => $baseUri];

        if ($this->config->get('debug', false)) {
            $clientOptions['debug'] = true;
        }

        if ($this->config->has('auth')) {
            $auth = (array)$this->config->get('auth');
            if (!empty($auth['username']) && !empty($auth['password'])) {
                $clientOptions['auth'] = [
                    $auth['username'],
                    $auth['password'],
                    isset($auth['type']) ? $auth['type'] : 'basic'
                ];
            }
        }

        if ($this->config->has('default_headers')) {
            $clientOptions['headers'] = (array)$this->config->get('default_headers');
        }

        if ($this->config->has('default_options')) {
            $clientOptions = array_merge($clientOptions, (array)$this->config->get('default_options')->toArray());
        }

        if ($this->config->has('default_query')) {
            $handler = HandlerStack::create();
            $handler->push(Middleware::mapRequest(
                function (RequestInterface $request) {
                    $uri = $request->getUri();
                    foreach ((array)$this->config->get('default_query')->toArray() as $param => $value) {
                        $uri = Uri::withQueryValue($uri, $param, $value);
                    }
                    return $request->withUri($uri);
                }
            ));
            $clientOptions['handler'] = $handler;
        }

        return new Client($clientOptions);
    }

    /**
     * Checks connection via HTTP(s).
     *
     * @return Status of the connection to the configured host
     */
    public function getStatus()
    {
        if ($this->config->has('fake_status')) {
            return new Status($this, $this->config->get('fake_status'));
        }

        if (!$this->config->has('status_test')) {
            return Status::unknown($this, ['message' => 'No status_test path specified']);
        }

        $path = $this->config->get('status_test');
        try {
            $info = [];
            $verbose = $this->config->get('status_verbose', true);

            $response = $this->getConnection()->get($path, [
                'on_stats' => function (TransferStats $stats) use (&$info, $verbose) {
                    if (!$verbose) {
                        return;
                    }
                    $info['effective_uri'] = (string)$stats->getEffectiveUri();
                    $info['transfer_time'] = $stats->getTransferTime();
                    $info = array_merge($info, $stats->getHandlerStats());
                    if ($stats->hasResponse()) {
                        $info['status_code'] = $stats->getResponse()->getStatusCode();
                    } else {
                        $errorData = $stats->getHandlerErrorData();
                        if (is_array($errorData) || is_string($errorData)) {
                            $info['handler_error_data'] = $errorData;
                        }
                    }
                }
            ]);

            $statusCode = $response->getStatusCode();
            if ($statusCode >= 200 && $statusCode < 300) {
                $msg['message'] = 'GET succeeded: ' . $path;
                if (!empty($info)) {
                    $msg['info'] = $info;
                }
                return Status::working($this, $msg);
            }

            return Status::failing(
                $this,
                [
                    'message' => 'GET failed: ' . $path,
                    'headers' => $response->getHeaders(),
                    'info' => $info
                ]
            );
        } catch (Exception $e) {
            error_log(
                '[' . static::CLASS . '] Error on "' . $path . '": ' . $e->getMessage() . "\n" . $e->getTraceAsString()
            );
            return Status::failing($this, ['message' => 'Error on "' . $path . '": ' . $e->getMessage()]);
        }
    }
}

<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue\db;

use yii\base\Exception;
use yii\db\Query;
use yii\queue\ExecEvent;
use yii\queue\InvalidJobException;
use yii\queue\RetryableJobInterface;

/**
 * Ordered Db Queue.
 */
class OrderedQueue extends Queue
{
    /**
     * @param ExecEvent $event
     * @return bool
     * @internal
     */
    public function handleError(ExecEvent $event)
    {
        $event->retry = $event->attempt < $this->attempts;
        if ($event->error instanceof InvalidJobException) {
            $event->retry = false;
        } elseif ($event->job instanceof RetryableJobInterface) {
            $event->retry = $event->job->canRetry($event->attempt, $event->error);
        }
        $this->trigger(self::EVENT_AFTER_ERROR, $event);

        // To detect failed jobs outside.
        return false;
    }

    /**
     * Listens queue and runs each job.
     *
     * @param bool $repeat whether to continue listening when queue is empty.
     * @param int $timeout number of seconds to sleep before next iteration.
     * @return null|int exit code.
     * @internal for worker command only
     * @since 2.0.2
     */
    public function run($repeat, $timeout = 0)
    {
        return $this->runWorker(function (callable $canContinue) use ($repeat, $timeout) {
            while ($canContinue()) {
                if ($payload = $this->reserve()) {
                    if ($this->handleMessage(
                        $payload['id'],
                        $payload['job'],
                        $payload['ttr'],
                        $payload['attempt']
                    )) {
                        $this->release($payload);
                    } else {
                        // Job failed, all the following ones should not start.
                        break;
                    }
                } elseif (!$repeat) {
                    break;
                } elseif ($timeout) {
                    sleep($timeout);
                }
            }
        });
    }

    /**
     * Takes one message from waiting list and reserves it for handling.
     *
     * @return array|false payload
     * @throws Exception in case it hasn't waited the lock
     */
    protected function reserve()
    {
        return $this->db->useMaster(function () {
            if (!$this->mutex->acquire(__CLASS__ . $this->channel, $this->mutexTimeout)) {
                throw new Exception('Has not waited the lock.');
            }

            try {
                $this->moveExpired();

                // Reserve one message
                $payload = (new Query())
                    ->from($this->tableName)
                    ->andWhere(['channel' => $this->channel])
                    ->andWhere('[[pushed_at]] <= :time - [[delay]]', [':time' => time()])
                    ->orderBy(['priority' => SORT_ASC, 'id' => SORT_ASC])
                    ->limit(1)
                    ->one($this->db);

                // Keeping the order.
                if (!empty($payload['reserved_at']) || $payload['attempt'] >= $this->attempts) {
                    $payload = false;
                }

                if (is_array($payload)) {
                    $payload['reserved_at'] = time();
                    $payload['attempt'] = (int) $payload['attempt'] + 1;
                    $this->db->createCommand()->update($this->tableName, [
                        'reserved_at' => $payload['reserved_at'],
                        'attempt' => $payload['attempt'],
                    ], [
                        'id' => $payload['id'],
                    ])->execute();

                    // pgsql
                    if (is_resource($payload['job'])) {
                        $payload['job'] = stream_get_contents($payload['job']);
                    }
                }
            } finally {
                $this->mutex->release(__CLASS__ . $this->channel);
            }

            return $payload;
        });
    }
}

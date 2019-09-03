module Beefheart.Internal
  ( dequeueOperations
  , fetchMetrics
  , indexingRunner
  , metricsRunner
  , queueMetricsFor
  , queueWatcher
  ) where

import RIO
import Control.Concurrent.STM.TBQueue (flushTBQueue, lengthTBQueue)
import Control.Monad.Loops (iterateM_)
import Data.Aeson (FromJSON)
import Data.Time.Clock.POSIX
import Database.V5.Bloodhound (BulkOperation, EsError, IndexName)
import Network.HTTP.Req hiding (header)

import qualified System.Metrics as EKG
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Gauge as Gauge
import qualified System.Metrics.Label as Label

import Beefheart.Elasticsearch
import Beefheart.Fastly
import Beefheart.Types
import Beefheart.Utils

-- |Small utility to keep an eye on our bulk operations queue.
queueWatcher
  :: (MonadIO m)
  => App         -- ^ Our RIO application env
  -> Gauge.Gauge -- ^ The EKG value we'll send measurements to
  -> m b         -- ^ Negligible return type
queueWatcher app gauge = do
  queueLength <- atomically $ lengthTBQueue (appQueue app)
  liftIO $ Gauge.set gauge $ fromIntegral queueLength
  sleepSeconds (queueMetricsWakeup $ appCli app)
  queueWatcher app gauge

-- |Self-contained IO action to regularly fetch and queue up metrics for storage
-- in Elasticsearch.
metricsRunner
  :: (MonadIO m, MonadReader env m, HasLogFunc env)
  => App
  -> (POSIXTime -> IndexName)
  -> Text
  -> m ()
metricsRunner app indexNamer service = do
  -- Create a metrics counter for this service.
  counter <- liftIO $ EKG.createCounter (metricN $ "requests-" <> service) (appEKG app)

  -- Fetch the service ID's details (to get the human-readable name)
  serviceDetails <- withRetries ifLeft $ fastlyReq FastlyRequest
    { apiKey       = (fastlyKey $ appEnv app)
    , timestampReq = Nothing
    , serviceId    = Just service
    , service      = ServiceAPI
    }

  case serviceDetails of
    Left err ->
      logError . display $ "Skipping service " <> service <> ": " <> tshow err
    Right serviceDetailsResponse -> do
      -- Before entering the metrics fetching loop, record the service's details in EKG.
      liftIO $ EKG.createLabel (metricN service) (appEKG app) >>= flip Label.set serviceName

      -- iterateM_ executes a monadic action (here, IO) and runs forever,
      -- feeding the return value into the next iteration, which fits well with
      -- our use case: keep hitting Fastly and feed the previous timestamp into
      -- the next request.
      iterateM_ (queueMetricsFor (fastlyBackoff $ appCli app) (appQueue app) toDocs getMetrics) 0
      where getMetrics = fetchMetrics counter (fastlyKey $ appEnv app) service
            toDocs = toBulkOperations indexNamer serviceName
            serviceName = name $ responseBody serviceDetailsResponse

-- |Higher-order function suitable to be fed into iterate that will be the main
-- metrics retrieval loop. Get a timestamp, fetch some metrics for that
-- timestamp, enqueue them, sleep, repeat.
queueMetricsFor
  :: (MonadIO m, MonadReader env m, HasLogFunc env)
  => Int -- ^ API backoff factor
  -> TBQueue BulkOperation -- ^ Our application's metrics queue.
  -> (Analytics -> [BulkOperation]) -- ^ A function to transform our metrics into ES bulk operations
  -> (POSIXTime -> m (Either HttpException (JsonResponse Analytics))) -- ^ Function to get metrics for a timestamp
  -> POSIXTime -- ^ The actual metrics timestamp we want
  -> m POSIXTime -- ^ Return the new POSIXTime for the subsequent request
queueMetricsFor backoff q f getter ts = do
  response <- getter ts
  case response of
    Right metrics -> do
      atomically $ mapM_ (writeTBQueue q) (f (responseBody metrics))
      sleepSeconds 1
      return $ timestamp $ responseBody metrics
    Left httpException -> do
      logError . display $
        ( "Error from Fastly: " <> tshow httpException <> ". "
        <> "Easing off the API for " <> tshow backoff <> " seconds."
        )
      sleepSeconds backoff
      return ts

-- |A self-contained indexing runner intended to be run within a thread. Wakes
-- up periodically to bulk index documents that it finds in our queue.
indexingRunner
  :: (MonadIO m, MonadThrow m, MonadReader env m, HasLogFunc env)
  => App
  -> m b
indexingRunner app = do
  -- See the comment on dequeueOperations for additional information, but
  -- tl;dr, we should always get _something_ from this action. Note that
  -- Elasticsearch will error out if we try and index nothing ([])
  docs <- atomically $ dequeueOperations $ appQueue app
  esResponse <- indexAnalytics (appBH app) docs
  case esResponse of
    Left esError -> logError $ display esError
    Right _ -> return ()
  -- Sleep for a time before performing another bulk operation.
  sleepSeconds (esFlushDelay $ appCli app)
  indexingRunner app

-- |Dumb instance just so we can log ES errors
instance Display EsError
  where textDisplay a = tshow a

-- |Flush Elasticsearch bulk operations that need to be indexed from our queue.
dequeueOperations
  :: TBQueue BulkOperation -- ^ Our thread-safe queue
  -> STM [BulkOperation]   -- ^ Monadic STM return value
dequeueOperations q = do
  -- STM paradigms in the Haskell world are very powerful but do require some
  -- context - here, if `check` fails, the transaction is restarted (it's a form
  -- of `retry` from the STM library.) However, the runtime will know that we
  -- tried to read from our queue when we bailed out to retry, so upon finding
  -- that the queue is empty, the transaction will then block until the queue is
  -- written to. This is why we don't need to wait/sleep before checking if the
  -- queue is empty again.
  queueIsEmpty <- isEmptyTBQueue q
  checkSTM $ not queueIsEmpty
  flushTBQueue q

-- |Retrieve real-time analytics from Fastly (with retries)
-- fetchMetrics
--   :: Counter.Counter -- ^ An EKG counter to track request activity
--   -> Text            -- ^ Fastly API key
--   -> Text            -- ^ ID of the Fastly service we want to inspect
--   -> POSIXTime       -- ^ Timestamp for analytics
--   -> IO (Either HttpException (JsonResponse Analytics))
fetchMetrics
  :: (MonadIO m, FromJSON a)
  => Counter.Counter
  -> Text
  -> Text
  -> POSIXTime
  -> m (Either HttpException (JsonResponse a))
fetchMetrics counter key service ts = do
  -- Immediately prior to the Fastly request, hit the counter.
  liftIO $ Counter.inc counter
  -- Perform actual API call to Fastly asking for metrics for a service.
  withRetries ifLeft $
    fastlyReq FastlyRequest
              { apiKey = key
              , timestampReq = Just ts
              , serviceId = Just service
              , service = AnalyticsAPI
              }

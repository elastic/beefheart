module Util
  ( containerCleanup
  , elasticsearchContainer
  )
where

import RIO

import Control.Exception.Safe
import Data.Char
import qualified Data.HashMap.Strict as HMS
import Data.Scientific
import Data.Time.Clock.POSIX (POSIXTime)
import Docker.Client
import Generic.Random
import Test.Tasty.QuickCheck

import Beefheart

-- In order to make automated testing easier, we tell the compiler how to
-- generate random values for our types.

-- |Helper to let us create purely random `Arbitrary` records.
instance Arbitrary Analytics where
  arbitrary = genericArbitrarySingle

-- |Although general implementations of this exist, we don't want crazy
-- unpredictable timestamps, so create a little arbitrary instance for a
-- POSIXTime that is in the general neighborhood.
instance Arbitrary POSIXTime where
  arbitrary = do
    -- Jan 1st 2018 to Jan 1st 2019 -- (easy magic numbers)
    n <- choose (1514764800 :: Double, 1546300800)
    return $ realToFrac n

instance Arbitrary Datacenter where
  arbitrary = genericArbitrarySingle

instance Arbitrary Metrics where
  arbitrary = genericArbitrarySingle

instance (Arbitrary a, Eq a, Hashable a, Arbitrary b) => Arbitrary (HashMap a b) where
  arbitrary = do
    HMS.fromList <$> listOf1 arbitrary

-- |We define a custom `Arbitrary` instance for `Scientific` values so that
-- numbers aren't unusably massive for elasticsearch.
instance Arbitrary Scientific where
  arbitrary = do
    c <- choose
      ( 0
      , ((2 ^ (32 :: Integer)))
      )
    return $ scientific c 1

-- |In order to generate String values without insane encoding, we generate
-- strings with a shape generally like those that Fastly tends to hand back
-- (capitalized strings)
--
-- Note that we include a language pragma here, but it's pretty safe since we're
-- overlapping a custom String type.
instance {-# OVERLAPPING #-} Arbitrary PointOfPresence where
  arbitrary = listOf1 (elements (filter isAlpha ['A' .. 'Z']))

-- These are some IO helpers to manage container standup/cleanup as part of
-- testing structure.

-- |Start an elasticsearch container on the local host via unix domain socket.
elasticsearchContainer ::
  (MonadUnliftIO m, MonadMask m) =>
  Text -- ^ Elasticsearch version
  -> m ContainerID -- ^ Resultant container ID
elasticsearchContainer v = do
  h <- unixHttpHandler "/var/run/docker.sock"
  runDockerT (defaultClientOpts, h) $ do
    cid <- createContainer myCreateOpts Nothing
    case cid of
        Left err -> RIO.error $ show err
        Right i -> do
            _ <- startContainer defaultStartOpts i
            return i
    where pb = PortBinding 9200 TCP [HostPort "0.0.0.0" 9200]
          myCreateOpts = addContainerEnv [EnvVar "discovery.type" "single-node"] $
                           addPortBinding pb $
                           defaultCreateOpts $ "docker.elastic.co/elasticsearch/elasticsearch:" <> v

-- |Helper to add on environment variables to a container startup
addContainerEnv :: [EnvVar] -> CreateOpts -> CreateOpts
addContainerEnv e c =
  -- All we do here is re-construct a `CreateOpts` value with the
  -- containerConfig.env value appended with the `e` environment variable pair
  -- we pass in.
  c { containerConfig = config { env = env' <> e } }
  where config = containerConfig c
        env' = env config

-- |Stop and remove a container ID.
containerCleanup :: ContainerID -> IO ()
containerCleanup cid = do
  h <- unixHttpHandler "/var/run/docker.sock"
  runDockerT (defaultClientOpts, h) $ do
    r <- stopContainer DefaultTimeout cid
    case r of
        Left err -> RIO.error $ "I failed to stop the container:" <> show err
        Right _ -> do
          d <- deleteContainer defaultContainerDeleteOpts cid
          case d of
            Left err' -> RIO.error $ "Couldn't delete container: " <> show err'
            Right _ -> return ()

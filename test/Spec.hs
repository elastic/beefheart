import ClassyPrelude

import Control.Retry
import Data.Aeson
import Database.V5.Bloodhound
import Docker.Client
import Network.HTTP.Client
import Test.Tasty
import Test.Tasty.HUnit

import Beefheart

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  testGroup "Tests"
  [ unitTests
  , integrationTests
  ]

unitTests :: TestTree
unitTests =
  testGroup "Unit tests"
  [ testCase "Aeson merging" $
    mergeAeson
    [ object [ "foo" .= ("bar" :: Text) ]
    , object [ "baz" .= ("bar" :: Text) ]
    ] @?= object [ "foo" .= ("bar" :: Text) , "baz" .= ("bar" :: Text) ]
  ]

integrationTests :: TestTree
integrationTests =
  withResource elasticsearchContainer containerCleanup $ \_cid ->
    testGroup "Integration Tests"
    [ testCase "Elasticsearch is UP" $ do
      resp <- recoverAll backoffThenGiveUp (\_exc -> runBH' getStatus)
      print resp
    ]
  where runBH' = withBH defaultManagerSettings (Server "http://localhost:9200")

elasticsearchContainer :: IO ContainerID
elasticsearchContainer = do
  h <- unixHttpHandler "/var/run/docker.sock"
  runDockerT (defaultClientOpts, h) $ do
    cid <- createContainer myCreateOpts Nothing
    case cid of
        Left err -> error $ show err
        Right i -> do
            _ <- startContainer defaultStartOpts i
            return i
    where pb = PortBinding 9200 TCP [HostPort "0.0.0.0" 9200]
          myCreateOpts = addPortBinding pb $ defaultCreateOpts "elasticsearch:6.5.1"

containerCleanup :: ContainerID -> IO ()
containerCleanup cid = do
  h <- unixHttpHandler "/var/run/docker.sock"
  runDockerT (defaultClientOpts, h) $ do
    r <- stopContainer DefaultTimeout cid
    case r of
        Left err -> error $ "I failed to stop the container:" <> show err
        Right _ -> do
          d <- deleteContainer defaultContainerDeleteOpts cid
          case d of
            Left err' -> error $ "Couldn't delete container: " <> show err'
            Right _ -> return ()

import ClassyPrelude

import Control.Retry
import Data.Aeson
import Database.V5.Bloodhound
import Network.HTTP.Client
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck

import Beefheart
import Util

main :: IO ()
main = do
  -- Before launching, create an HTTP request manager that we trickle down into
  -- tests.
  manager <- newManager defaultManagerSettings
  defaultMain $ tests manager

-- |High-level test entrypoint for unit tests and integration tests.
tests :: Manager -> TestTree
tests m =
  testGroup "Tests"
  [ unitTests
  , integrationTests m
  ]

-- |Simple unit tests
unitTests :: TestTree
unitTests =
  testGroup "Unit tests"
  [ testCase "Aeson merging" $
    mergeAeson
    [ object [ "foo" .= ("bar" :: Text) ]
    , object [ "baz" .= ("bar" :: Text) ]
    ] @?= object [ "foo" .= ("bar" :: Text) , "baz" .= ("bar" :: Text) ]
  ]

-- |Tests that do a bit more, i.e. talk to Elasticsearch to verify indexing
-- works.
integrationTests :: Manager -> TestTree
integrationTests m =
  withResource (elasticsearchContainer "6.5.4") containerCleanup $ \_cid ->
    testGroup "Integration Tests"
    -- First, confirm the API is responsive.
    [ testCase "Elasticsearch is UP" $ do
      resp <- recoverAll backoffThenGiveUp (\_exc -> runBH bh getStatus)
      assertBool "Elasticsearch container is responsive" (isJust resp)
    , after AllSucceed "Elasticsearch is UP" $
    -- Then, try installing the index template.
      testCase "Install template" $ do
        bootstrapElasticsearch bh "fastly"
    , after AllSucceed "Install template" $
    -- Finally, try indexing a set of random `Analytics`
      testCase "Documents can be indexed" $ do
        let toBulk = toBulkOperations "fastly" "%Y" "myservice"
        analytics <- generate arbitrary :: IO ([Analytics])
        let documents = analytics >>= toBulk
        bulkResp <- indexAnalytics bh documents
        case bulkResp of
          Left _ -> assertFailure $ "Bad response from Elasticsearch: " <> show bulkResp
          Right esResp -> do
            assertBool "Indexing errors" (not $ bulkErrors esResp)
            assertEqual "Missing documents" (length documents) (length $ items esResp)
    ]
  where bh = mkBHEnv (Server "http://localhost:9200") m

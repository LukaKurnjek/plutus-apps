{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE QuasiQuotes     #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE TemplateHaskell #-}
{- | A model base test for indexers.
 -
 - The idea is that we have a naive modelof indexer ('Model') that stores information in a list
 - (from the most recent to the oldest).
 -
 - When we want to test an indexer implementation, we write a 'IndexerTestRunner' for this indexer.
 -
 - It allows us to run the chain of events on both the model and the indexer.
 - We can then query both and compare their results (using 'behaveLikeModel').
 -
 - A set of basic properties for indexers is available and ready to run once you have an `IndexerTestRunner`.
 -
 - A limitation is that our model use 'TestPoint' (a wrapper for 'Int')  as a 'Point' type.
 - If you want to test an indexer with your own events, that have their own 'Point' type,
 - you'll probably need to wrap the event in a @newtype@ to use 'TestPoint' as a 'Point'.
-}
module Marconi.Core.Spec.Experiment
    (
    -- * The test suite
      indexingTestGroup
    -- ** individual tests
    , storageBasedModelProperty
    , lastSyncBasedModelProperty
    -- * Mock chain
    , DefaultChain
        , defaultChain
    , ForwardChain
        , forwardChain
    -- ** Events
    , Item (..)
    , TestPoint (..)
    , TestEvent (..)
    -- ** Generators
    , genInsert
    , genRollback
    , genItem
    -- * Model
    , IndexerModel (..)
        , model
    , runModel
    -- ** Mapping
    , IndexerTestRunner
        , indexerRunner
        , indexerGenerator
    -- ** Testing
    , behaveLikeModel
    -- * Instances
    , listIndexerRunner
    , sqliteIndexerRunner
    , mixedLowMemoryIndexerRunner
    , mixedHighMemoryIndexerRunner
    -- ** Instances internal
    , initSQLite
    , sqliteModelIndexer
    ) where

import Control.Lens (makeLenses, use, view, views, (%~), (.=), (^.))

import Control.Monad (foldM, replicateM)
import Control.Monad.Except (MonadError)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State (StateT, evalStateT, get)

import Data.Foldable (Foldable (foldl'))
import Data.Function ((&))

import GHC.Generics (Generic)

import Test.QuickCheck (Arbitrary, Gen, Property, (===))
import Test.QuickCheck qualified as Test
import Test.QuickCheck.Monadic (PropertyM)
import Test.QuickCheck.Monadic qualified as GenM

import Test.Tasty qualified as Tasty
import Test.Tasty.QuickCheck qualified as Tasty

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Except (ExceptT)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))

import Data.Either (fromRight)
import Data.Maybe (listToMaybe)

import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.FromField (FromField)
import Database.SQLite.Simple.ToField (ToField)

import Marconi.Core.Experiment qualified as Core

newtype TestPoint = TestPoint { unwrapTestPoint :: Int }
    deriving stock (Generic)
    deriving newtype (Eq, Ord, Enum, Num, Real, Integral, Show, FromField, ToField)
    deriving anyclass (SQL.FromRow)
    deriving anyclass (SQL.ToRow)

instance Core.HasGenesis TestPoint where
    genesis = 0

-- | We simplify the events to either `Insert` or `Rollback`
data Item event
    = Insert !TestPoint !event
    | Rollback !TestPoint
    deriving stock Show

-- | 'GenState' is used in generators of chain events to keep track of the latest slot
newtype GenState = GenState { _slotNo :: TestPoint }
    deriving stock Show

makeLenses 'GenState

-- | Generate an insert at the given slot
genInsert :: Arbitrary event => GenState -> Gen (Item event)
genInsert s = do
    xs <- Test.arbitrary
    pure $ Insert (s ^. slotNo + 1) xs

-- | Generate a rollback, 'GenState' set the maximal depth of the rollback
genRollback :: GenState -> Gen (Item event)
genRollback s = do
    n <- TestPoint <$> Test.choose (0, unwrapTestPoint $ s ^. slotNo)
    pure $ Rollback n

-- | Generate an insert or a rollback, rollback depth is uniform on the chain length
genItem
    :: Arbitrary event
    => Word -- ^ rollback frequency (insert weight is 100 - rollback frequency)
    -> StateT GenState Gen (Item event)
genItem f = do
    s <- get
    no <- use slotNo
    let f' = if no > 0 then f else 0 -- no rollback on genesis
    item <- lift $ Test.frequency
        [ (fromIntegral f',  genRollback s)
        , (100 - fromIntegral f', genInsert s)
        ]
    case item of
        Insert no' _ -> slotNo .= no'
        Rollback n   -> slotNo .= n
    pure item

-- | Chain events with 10% of rollback
newtype DefaultChain event = DefaultChain {_defaultChain :: [Item event]}

makeLenses 'DefaultChain

instance Arbitrary event => Arbitrary (DefaultChain event) where

    arbitrary = Test.sized $ \n -> do
        DefaultChain <$> evalStateT (replicateM n (genItem 10)) (GenState Core.genesis)

-- | Chain events without any rollback
newtype ForwardChain event = ForwardChain {_forwardChain :: [Item event]}

makeLenses 'ForwardChain

instance Arbitrary event => Arbitrary (ForwardChain event) where

    arbitrary = Test.sized $ \n -> do
        ForwardChain <$> evalStateT (replicateM n (genItem 0)) (GenState Core.genesis)

-- ** Event instances

newtype TestEvent = TestEvent Int
    deriving newtype (Arbitrary, Eq, Ord, Show, Num, FromField, ToField)

type instance Core.Point TestEvent = TestPoint

-- * Model

newtype IndexerModel e = IndexerModel {_model :: [(TestPoint, e)]}
    deriving stock Show

makeLenses ''IndexerModel

-- Build a model for the given chain of events
runModel :: [Item event] -> IndexerModel event
runModel = let

    modelStep :: IndexerModel event -> Item event -> IndexerModel event
    modelStep m (Insert w xs) = m & model %~ ((w,xs):)
    modelStep m (Rollback n) =
        m & model %~ dropWhile ((> n) . fst)

    in foldl' modelStep (IndexerModel [])

-- | Used to map an indexer to a model
data IndexerTestRunner m event indexer
    = IndexerTestRunner
        { _indexerRunner    :: !(PropertyM m Property -> Property)
        , _indexerGenerator :: !(m (indexer event))
        }

makeLenses ''IndexerTestRunner

-- | Compare an execution on the base model and one on the indexer
compareToModelWith
    :: Monad m
    => Show event
    => Core.Point event ~ TestPoint
    => Core.IsIndex (ExceptT Core.IndexError m) event indexer
    => Core.Rewindable m event indexer
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> (IndexerModel event -> a)
    -> (indexer event -> m a)
    -> (a -> a -> Property)
    -> Property
compareToModelWith genChain runner modelComputation indexerComputation prop
    = let
        rightToMaybe = either (const Nothing) Just
        process = \case
            Insert ix evt -> MaybeT . fmap rightToMaybe . Core.index' (Core.TimedEvent ix evt)
            Rollback n    -> MaybeT . Core.rewind n
        r = runner ^. indexerRunner
        genIndexer = runner ^. indexerGenerator
    in Test.forAll genChain $ \chain -> r $ do
        initialIndexer <- GenM.run genIndexer
        indexer <- GenM.run $ runMaybeT $ foldM (flip process) initialIndexer chain
        iResult <- GenM.run $ traverse indexerComputation indexer
        let model' = runModel chain
            mResult = modelComputation model'
        GenM.stop . maybe
            (failWith "invalid rewind")
            (`prop` mResult)
            $ iResult

-- | Compare an execution on the base model and one on the indexer
behaveLikeModel
    :: Eq a
    => Show a
    => Show event
    => Monad m
    => Core.Point event ~ TestPoint
    => Core.IsIndex (ExceptT Core.IndexError m) event indexer
    => Core.Rewindable m event indexer
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> (IndexerModel event -> a)
    -> (indexer event -> m a)
    -> Property
behaveLikeModel genChain runner modelComputation indexerComputation
    = compareToModelWith genChain runner modelComputation indexerComputation (===)

-- | A test tree for the core functionalities of an indexer
indexingTestGroup
    :: ( Core.Rewindable m TestEvent indexer
    , Core.IsIndex (ExceptT Core.IndexError m) TestEvent indexer
    , Core.IsSync m TestEvent indexer
    , Core.Queryable
        (ExceptT (Core.QueryError (Core.EventsMatchingQuery TestEvent)) m)
        TestEvent
        (Core.EventsMatchingQuery TestEvent) indexer
    , Monad m
    ) => String -> IndexerTestRunner m TestEvent indexer -> Tasty.TestTree
indexingTestGroup indexerName runner
    = Tasty.testGroup (indexerName <> " core properties")
        [ Tasty.testGroup "Check storage"
            [ Tasty.testProperty "it stores events without rollback"
                $ Test.withMaxSuccess 20000
                $ storageBasedModelProperty (view forwardChain <$> Test.arbitrary) runner
            , Tasty.testProperty "it stores events with rollbacks"
                $ Test.withMaxSuccess 10000
                $ storageBasedModelProperty (view defaultChain <$> Test.arbitrary) runner
            ]
        , Tasty.testGroup "Check lastSync"
            [ Tasty.testProperty "in a chain without rollback"
                $ Test.withMaxSuccess 20000
                $ lastSyncBasedModelProperty (view forwardChain <$> Test.arbitrary) runner
            , Tasty.testProperty "in a chain with rollbacks"
                $ Test.withMaxSuccess 10000
                $ lastSyncBasedModelProperty (view defaultChain <$> Test.arbitrary) runner
            ]
        ]

storageBasedModelProperty
    ::
    ( Core.Rewindable m event indexer
    , Core.IsIndex (ExceptT Core.IndexError m) event indexer
    , Core.IsSync m event indexer
    , Core.Point event ~ TestPoint
    , Monad m
    , Show event
    , Eq event
    , Core.Queryable (ExceptT (Core.QueryError (Core.EventsMatchingQuery event)) m) event (Core.EventsMatchingQuery event) indexer
    )
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> Property
storageBasedModelProperty gen runner
    = let

        indexerEvents indexer = do
            p <- Core.lastSyncPoint indexer
            fmap (view Core.event)
                . fromRight []
                <$> Core.query' p Core.allEvents indexer

    in behaveLikeModel
        gen
        runner
        (views model (fmap snd))
        indexerEvents

lastSyncBasedModelProperty
    ::
    ( Core.Rewindable m event indexer
    , Core.IsIndex (ExceptT Core.IndexError m) event indexer
    , Core.IsSync m event indexer
    , Core.Point event ~ TestPoint
    , Monad m
    , Show event
    )
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> Property
lastSyncBasedModelProperty gen runner
    = behaveLikeModel
        gen
        runner
        (views model (maybe Core.genesis fst . listToMaybe))
        Core.lastSyncPoint

-- | A runner for a 'ListIndexer'
listIndexerRunner
    :: Core.HasGenesis (Core.Point e)
    => IndexerTestRunner IO e Core.ListIndexer
listIndexerRunner
    = IndexerTestRunner
        GenM.monadicIO
        (pure Core.listIndexer)

failWith :: String -> Property
failWith message = Test.counterexample message False


initSQLite :: IO SQL.Connection
initSQLite = do
    con <- SQL.open ":memory:"

    SQL.execute_ con
        " CREATE TABLE index_model \
        \   ( point INT NOT NULL   \
        \   , value INT NOT NULL   \
        \   )                      "

    pure con

type instance Core.InsertRecord TestEvent = [(TestPoint, TestEvent)]

sqliteModelIndexer :: SQL.Connection -> Core.SQLiteIndexer TestEvent
sqliteModelIndexer con
    = Core.singleInsertSQLiteIndexer con
        (\t -> (t ^. Core.point, t ^. Core.event))
        "INSERT INTO index_model VALUES (?, ?)"

instance MonadIO m => Core.Rewindable m TestEvent Core.SQLiteIndexer where

    rewind = Core.rewindSQLiteIndexerWith "DELETE FROM index_model WHERE point > ?"

instance
    ( MonadIO m
    , MonadError (Core.QueryError (Core.EventsMatchingQuery TestEvent)) m
    ) =>
    Core.Queryable m TestEvent (Core.EventsMatchingQuery TestEvent) Core.SQLiteIndexer where

    query = let

        rowToResult (Core.EventsMatchingQuery predicate)
            = fmap (uncurry Core.TimedEvent)
            . filter (predicate . snd)

        in Core.querySQLiteIndexerWith
            (\p _ -> [":point" SQL.:= p])
            " SELECT point, value \
            \ FROM index_model \
            \ WHERE point <= :point \
            \ ORDER BY point DESC"
            rowToResult

-- | A runner for a 'SQLiteIndexer'
sqliteIndexerRunner
    :: IndexerTestRunner IO TestEvent Core.SQLiteIndexer
sqliteIndexerRunner
    = IndexerTestRunner
        GenM.monadicIO
        (sqliteModelIndexer <$> initSQLite)

mixedModelLowMemoryIndexer
    :: SQL.Connection
    -> Core.MixedIndexer Core.SQLiteIndexer Core.ListIndexer TestEvent
mixedModelLowMemoryIndexer con
    = Core.mixedIndexer
        10
        2
        (sqliteModelIndexer con)
        Core.listIndexer

mixedModelHighMemoryIndexer
    :: SQL.Connection
    -> Core.MixedIndexer Core.SQLiteIndexer Core.ListIndexer TestEvent
mixedModelHighMemoryIndexer con
    = Core.mixedIndexer
        8192
        4096
        (sqliteModelIndexer con)
        Core.listIndexer

-- | A runner for a 'SQLiteIndexer'
mixedLowMemoryIndexerRunner
    :: IndexerTestRunner IO TestEvent (Core.MixedIndexer Core.SQLiteIndexer Core.ListIndexer)
mixedLowMemoryIndexerRunner
    = IndexerTestRunner
        GenM.monadicIO
        (mixedModelLowMemoryIndexer <$> initSQLite)

-- | A runner for a 'SQLiteIndexer'
mixedHighMemoryIndexerRunner
    :: IndexerTestRunner IO TestEvent (Core.MixedIndexer Core.SQLiteIndexer Core.ListIndexer)
mixedHighMemoryIndexerRunner
    = IndexerTestRunner
        GenM.monadicIO
        (mixedModelHighMemoryIndexer <$> initSQLite)

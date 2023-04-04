{-# LANGUAGE LambdaCase      #-}
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
 - A limitation is that our model use 'Word' as a 'Point' type.
 - If you want to test an indexer with your own events, that have their own 'Point' type,
 - you'll probably need to wrap the event in a @newtype@ to use 'Word' as a 'Point'.
-}
module Marconi.Core.Spec.Experiment
    (
    -- * The test suite
      testIndexer
    -- * Mock chain
    , DefaultChain
        , defaultChain
    , ForwardChain
        , forwardChain
    -- ** Events
    , Item (..)
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
    ) where

import Control.Lens (makeLenses, use, view, views, (%~), (-=), (.=), (^.))

import Control.Monad (foldM, replicateM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State (StateT, evalStateT, get)

import Data.Foldable (Foldable (foldl'))
import Data.Function ((&))
import Data.List (inits)

import Test.QuickCheck (Arbitrary, Gen, Property, (===))
import Test.QuickCheck qualified as Test
import Test.QuickCheck.Monadic (PropertyM)
import Test.QuickCheck.Monadic qualified as GenM

import Test.Tasty qualified as Tasty
import Test.Tasty.QuickCheck qualified as Tasty

import Control.Monad.Trans.Except (ExceptT)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Either (fromRight)
import Data.Maybe (listToMaybe)
import Marconi.Core.Experiment (EventsMatchingQuery, IndexError, IsIndex (index), IsSync (lastSyncPoint), ListIndexer,
                                Point, QueryError, Queryable, Result (filteredEvents), Rewindable (rewind),
                                TimedEvent (TimedEvent), allEvents, listIndexer, query')



-- | We simplify the events to either `Insert` or `Rollback`
data Item event
    = Insert !Word !event
    | Rollback !Word
    deriving stock Show

-- | 'GenState' is used in generators of chain events to keep track of the latest slot
newtype GenState = GenState { _slotNo :: Word }
    deriving stock Show

makeLenses 'GenState

-- | Generate an insert at the given slot
genInsert :: Arbitrary event => GenState -> Gen (Item event)
genInsert s = do
    xs <- Test.arbitrary
    pure $ Insert (s ^. slotNo) xs

-- | Generate a rollback, 'GenState' set the maximal depth of the rollback
genRollback :: GenState -> Gen (Item event)
genRollback s = do
    n <- Test.choose (0, s ^. slotNo)
    pure $ Rollback n

-- | Generate an insert or a rollback, rollback depth is uniform on the chain length
genItem
    :: Arbitrary event
    => Word -- ^ rollback frequency (insert weight is 100 - rollback frequency)
    -> StateT GenState Gen (Item event)
genItem f = do
    s <- get
    no <- use slotNo
    let f' = if no > 0 then f else 0
    item <- lift $ Test.frequency
        [ (fromIntegral f',  genRollback s)
        , (100 - fromIntegral f', genInsert s)
        ]
    case item of
        Insert no' _ -> slotNo .= no'
        Rollback n   -> slotNo -= n
    pure item

-- | Chain events with 10% of rollback
newtype DefaultChain event = DefaultChain {_defaultChain :: [Item event]}

makeLenses 'DefaultChain

instance Arbitrary event => Arbitrary (DefaultChain event) where

    arbitrary = Test.sized $ \n -> do
        DefaultChain <$> evalStateT (replicateM n (genItem 10)) (GenState 0)

    shrink = defaultChain inits

-- | Chain events without any rollback
newtype ForwardChain event = ForwardChain {_forwardChain :: [Item event]}

makeLenses 'ForwardChain

instance Arbitrary event => Arbitrary (ForwardChain event) where

    arbitrary = Test.sized $ \n -> do
        ForwardChain <$> evalStateT (replicateM n (genItem 0)) (GenState 0)

    shrink = forwardChain inits

-- ** Event instances

type instance Point Int = Word

-- * Model

newtype IndexerModel e = IndexerModel {_model :: [(Word, e)]}
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
        , _indexerGenerator :: !(Gen (indexer event))
        }

makeLenses ''IndexerTestRunner

-- | Compare an execution on the base model and one on the indexer
compareToModelWith
    :: Monad m
    => Show event
    => Show (indexer event)
    => Point event ~ Word
    => IsIndex m event indexer
    => Rewindable m event indexer
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> (IndexerModel event -> a)
    -> (indexer event -> m a)
    -> (a -> a -> Property)
    -> Property
compareToModelWith genChain mapper modelComputation indexerComputation prop
    = let
        process = \case
            Insert ix event -> MaybeT . fmap Just . index (TimedEvent ix event)
            Rollback n      -> MaybeT . rewind n
        runner = mapper ^. indexerRunner
        genIndexer = mapper ^. indexerGenerator
    in Test.forAll genChain $ \chain -> runner $ do
        initialIndexer <- GenM.pick genIndexer
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
    => Show (indexer event)
    => Point event ~ Word
    => IsIndex m event indexer
    => Rewindable m event indexer
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> (IndexerModel event -> a)
    -> (indexer event -> m a)
    -> Property
behaveLikeModel genChain mapper modelComputation indexerComputation
    = compareToModelWith genChain mapper modelComputation indexerComputation (===)

-- | A test tree for the core functionalities of an indexer
testIndexer
    :: ( Rewindable m Int indexer
    , IsIndex m Int indexer
    , IsSync m Int indexer
    , Show (indexer Int)
    , Queryable (ExceptT (QueryError (EventsMatchingQuery Int)) m) Int (EventsMatchingQuery Int) indexer
    ) => String -> IndexerTestRunner m Int indexer -> Tasty.TestTree
testIndexer indexerName mapper
    = Tasty.testGroup (indexerName <> " core properties")
        [ Tasty.testGroup "Check storage"
            [ Tasty.testProperty "it stores events without rollback"
                $ Test.withMaxSuccess 1000
                $ storageBasedModelProperty (view forwardChain <$> Test.arbitrary) mapper
            , Tasty.testProperty "it stores events with rollbacks"
                $ Test.withMaxSuccess 1000
                $ storageBasedModelProperty (view defaultChain <$> Test.arbitrary) mapper
            ]
        , Tasty.testGroup "Check lastSync"
            [ Tasty.testProperty "in a chain without rollback"
                $ Test.withMaxSuccess 1000
                $ lastSyncBasedModelProperty (view forwardChain <$> Test.arbitrary) mapper
            , Tasty.testProperty "in a chain with rollbacks"
                $ Test.withMaxSuccess 1000
                $ lastSyncBasedModelProperty (view defaultChain <$> Test.arbitrary) mapper
            ]
        ]

storageBasedModelProperty
    ::
    ( Rewindable m event indexer
    , IsIndex m event indexer
    , IsSync m event indexer
    , Point event ~ Word
    , Show (indexer event)
    , Show event
    , Eq event
    , Queryable (ExceptT (QueryError (EventsMatchingQuery event)) m) event (EventsMatchingQuery event) indexer
    )
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> Property
storageBasedModelProperty gen mapper
    = let

        indexerEvents indexer = do
            mp <- lastSyncPoint indexer
            maybe
                (pure [])
                (\p -> either (const []) filteredEvents <$> query' p allEvents indexer)
                mp

    in behaveLikeModel
        gen
        mapper
        (views model (fmap snd))
        indexerEvents

lastSyncBasedModelProperty
    ::
    ( Rewindable m event indexer
    , IsIndex m event indexer
    , IsSync m event indexer
    , Point event ~ Word
    , Show (indexer event)
    , Show event
    )
    => Gen [Item event]
    -> IndexerTestRunner m event indexer
    -> Property
lastSyncBasedModelProperty gen mapper
    = behaveLikeModel
        gen
        mapper
        (views model (fmap fst . listToMaybe))
        lastSyncPoint

-- | A runner for a `ListIndexer`
listIndexerRunner :: IndexerTestRunner (Either (IndexError ListIndexer Int)) e ListIndexer
listIndexerRunner
    = IndexerTestRunner
        (GenM.monadic $ fromRight (failWith "indexing error"))
        (pure listIndexer)

failWith :: String -> Property
failWith message = Test.counterexample message False

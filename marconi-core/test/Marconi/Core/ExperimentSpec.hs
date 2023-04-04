{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE TemplateHaskell #-}
module Marconi.Core.ExperimentSpec where

import Control.Lens (makeLenses, use, view, views, (%~), (-=), (.=), (^.))

import Control.Monad (foldM, replicateM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State (StateT, evalStateT, get)

import Data.Foldable (Foldable (foldl'))
import Data.Function ((&))
import Data.List (inits)

import Test.QuickCheck (Arbitrary, Gen, Property, Testable (property), (===))
import Test.QuickCheck qualified as Test
import Test.QuickCheck.Monadic (PropertyM)
import Test.QuickCheck.Monadic qualified as GenM

import Test.Tasty qualified as Tasty
import Test.Tasty.QuickCheck qualified as Tasty

import Control.Monad.Trans.Except (ExceptT)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Either (fromRight)
import Marconi.Core.Experiment (EventsMatchingQuery, IndexError, IsIndex (index), IsSync (lastSyncPoint), ListIndexer,
                                Point, QueryError, Queryable, Result (filteredEvents), Rewindable (rewind),
                                TimedEvent (TimedEvent), allEvents, listIndexer, query')



-- * Events

data Item event
    = Insert !Word !event
    | Rollback !Word
    deriving stock Show

newtype GenState = GenState { _slotNo :: Word }
    deriving stock Show

makeLenses 'GenState

genInsert :: Arbitrary event => GenState -> Gen (Item event)
genInsert s = do
    xs <- Test.arbitrary
    pure $ Insert (s ^. slotNo) xs

genRollback :: GenState -> Gen (Item event)
genRollback s = do
    n <- Test.choose (0, s ^. slotNo)
    pure $ Rollback n

genItem
    :: Arbitrary event
    => Int -- ^ rollback frequency (insert weight is 100)
    -> StateT GenState Gen (Item event)
genItem f = do
    s <- get
    no <- use slotNo
    let f' = if no > 0 then f else 0
    item <- lift $ Test.frequency
        [ (f',  genRollback s)
        , (100, genInsert s)
        ]
    case item of
        Insert no' _ -> slotNo .= no'
        Rollback n   -> slotNo -= n
    pure item

newtype DefaultChain event = DefaultChain {_defaultChain :: [Item event]}

makeLenses 'DefaultChain

instance Arbitrary event => Arbitrary (DefaultChain event) where

    arbitrary = Test.sized $ \n -> do
        DefaultChain <$> evalStateT (replicateM n (genItem 10)) (GenState 0)

    shrink = defaultChain inits

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

modelStep :: IndexerModel event -> Item event -> IndexerModel event
modelStep m (Insert w xs) = m & model %~ ((w,xs):)
modelStep m (Rollback n) =
    m & model %~ dropWhile ((> n) . fst)

runModel :: [Item event] -> IndexerModel event
runModel = foldl' modelStep (IndexerModel [])

data ModelMapper m event indexer
    = ModelMapper
        { _indexerRunner    :: !(PropertyM m Property -> Property)
        , _indexerGenerator :: !(Gen (indexer event))
        }

makeLenses ''ModelMapper

behaveLikeModelM
    :: Monad m
    => Eq a
    => Show a
    => Show event
    => Show (indexer event)
    => Point event ~ Word
    => IsIndex m event indexer
    => Rewindable m event indexer
    => Gen [Item event]
    -> ModelMapper m event indexer
    -> (IndexerModel event -> a)
    -> (indexer event -> m a)
    -> Property
behaveLikeModelM genChain mapper modelComputation indexerComputation
    = let
        process = \case
            Insert ix event -> MaybeT . fmap Just . index (TimedEvent ix event)
            Rollback n      -> MaybeT . rewind n
        runner = mapper ^. indexerRunner
        genIndexer = mapper ^. indexerGenerator
    in runner $ GenM.forAllM genChain $ \chain -> do
        initialIndexer <- GenM.pick genIndexer
        indexer <- GenM.run $ runMaybeT $ foldM (flip process) initialIndexer chain
        iResult <- GenM.run $ traverse indexerComputation indexer
        let model' = runModel chain
            mResult = modelComputation model'
        GenM.stop $ iResult === Just mResult

testIndexer
    :: ( Rewindable m Int indexer
    , IsIndex m Int indexer
    , IsSync m Int indexer
    , Show (indexer Int)
    , Queryable (ExceptT (QueryError (EventsMatchingQuery Int)) m) Int (EventsMatchingQuery Int) indexer
    ) => ModelMapper m Int indexer -> Tasty.TestTree
testIndexer mapper
    = Tasty.testGroup "Check core indexer properties"
        [ Tasty.testProperty "it stores events without rollback"
            $ Test.withMaxSuccess 1000
            $ insertBasedModelProperty (view forwardChain <$> Test.arbitrary) mapper
        , Tasty.testProperty "it stores events with rollbacks"
            $ Test.withMaxSuccess 1000
            $ insertBasedModelProperty (view defaultChain <$> Test.arbitrary) mapper
        ]

insertBasedModelProperty
    ::
    ( Rewindable m Int indexer
    , IsIndex m Int indexer
    , IsSync m Int indexer
    , Point Int ~ Word
    , Show (indexer Int)
    , Queryable (ExceptT (QueryError (EventsMatchingQuery Int)) m) Int (EventsMatchingQuery Int) indexer
    )
    => Gen [Item Int]
    -> ModelMapper m Int indexer
    -> Property
insertBasedModelProperty gen mapper
    = let

        indexerEvents indexer = do
            mp <- lastSyncPoint indexer
            maybe
                (pure [])
                (\p -> either (const []) filteredEvents <$> query' p allEvents indexer)
                mp

    in behaveLikeModelM
        gen
        mapper
        (views model (fmap snd))
        indexerEvents

listIndexerMapper :: ModelMapper (Either (IndexError ListIndexer Int)) e ListIndexer
listIndexerMapper
    = ModelMapper (GenM.monadic $ fromRight (property False)) (pure listIndexer)


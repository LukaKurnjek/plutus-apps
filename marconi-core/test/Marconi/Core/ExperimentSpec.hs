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
import Data.Maybe (listToMaybe)
import Marconi.Core.Experiment (EventsMatchingQuery, IndexError, IsIndex (index), IsSync (lastSyncPoint), ListIndexer,
                                Point, QueryError, Queryable, Result (filteredEvents), Rewindable (rewind),
                                TimedEvent (TimedEvent), allEvents, listIndexer, query')



-- * Events


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
data ModelMapper m event indexer
    = ModelMapper
        { _indexerRunner    :: !(PropertyM m Property -> Property)
        , _indexerGenerator :: !(Gen (indexer event))
        }

makeLenses ''ModelMapper

-- | Compare an execution on the base model and one on the indexer
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
    in Test.forAll genChain $ \chain -> runner $ do
        initialIndexer <- GenM.pick genIndexer
        indexer <- GenM.run $ runMaybeT $ foldM (flip process) initialIndexer chain
        iResult <- GenM.run $ traverse indexerComputation indexer
        let model' = runModel chain
            mResult = modelComputation model'
        GenM.stop $ iResult === Just mResult

-- | A test tree for the core functionalities of an indexer
testIndexer
    :: ( Rewindable m Int indexer
    , IsIndex m Int indexer
    , IsSync m Int indexer
    , Show (indexer Int)
    , Queryable (ExceptT (QueryError (EventsMatchingQuery Int)) m) Int (EventsMatchingQuery Int) indexer
    ) => ModelMapper m Int indexer -> Tasty.TestTree
testIndexer mapper
    = Tasty.testGroup "Check core indexer properties"
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
    -> ModelMapper m event indexer
    -> Property
storageBasedModelProperty gen mapper
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
    -> ModelMapper m event indexer
    -> Property
lastSyncBasedModelProperty gen mapper
    = behaveLikeModelM
        gen
        mapper
        (views model (fmap fst . listToMaybe))
        lastSyncPoint

listIndexerMapper :: ModelMapper (Either (IndexError ListIndexer Int)) e ListIndexer
listIndexerMapper
    = ModelMapper (GenM.monadic $ fromRight (property False)) (pure listIndexer)

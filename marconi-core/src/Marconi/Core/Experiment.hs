{- |
 This module propose an alternative to the index implementation proposed in 'RewindableIndex.Storable'.

 The point we wanted to address are the folowing:

   - 'Storable' implementation is designed in a way that strongly promote indexer that rely on a mix of database and
     in-memory storage. While this implementation makes sense in most of the case we may want:

        - full in-memory indexer
        - indexer backed by a single file
        - mock indexer, for testing purpose, with predefined behaviour
        - be able to easily move to another kind of storage
        - implement in-memory/database storage that rely on other query heuristic

   - We want to be able to compose easily indexers to build new ones. For example, the original indexer design can be
     seen as the combination of two indexers, a full in-memory indexer, and a full in database indexer.

   - The original implementation considered the 'StorablePoint' as a data that can be derived from the 'Event', leading
     to the need of the design of synthetic events to deal with indexer that didn't index enough data.

   - In marconi, the original design use an exotic callback design to handle `MVar` modification, we wanted to address
     this point as well.

-}
module Marconi.Core.Experiment where

import Control.Concurrent (QSemN)
import Control.Concurrent qualified as Con
import Control.Lens (makeLenses, view, (%~), (&), (.~), (<<.~), (^.))
import Control.Monad (forever)

import Control.Concurrent qualified as STM
import Control.Concurrent.STM (TChan, TMVar)
import Control.Concurrent.STM qualified as STM
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Foldable (foldlM, foldrM, traverse_)
import Data.List (intersect)
import Database.SQLite.Simple qualified as SQL

data family Event desc
data family Query desc
data family Result desc
type family Point desc

data QueryValidity p = Latest | Exactly !p | AtLeast !p | LatestIn !p !p

lowerBound :: QueryValidity p -> Maybe p
lowerBound Latest         = Nothing
lowerBound (Exactly x)    = pure x
lowerBound (AtLeast x)    = pure x
lowerBound (LatestIn x _) = pure x

upperBound :: QueryValidity p -> Maybe p
upperBound (LatestIn _ x) = pure x
upperBound (Exactly x)    = pure x
upperBound _              = Nothing

class Monad m => IsIndex indexer desc m where

    -- | Store a point in time, associated to an event in an indexer
    insert :: Eq (Point desc) =>
        Point desc -> Event desc -> indexer desc -> m (indexer desc)

    insertAll :: (Eq (Point desc), Foldable f) =>
        f (Point desc, Event desc) -> indexer desc -> m (indexer desc)
    insertAll = flip (foldrM (uncurry insert))

    -- | Last sync of the indexer
    lastSyncPoint :: indexer desc -> m (Point desc)


-- | The indexer can answer a Query to produce the corresponding result
class Queryable indexer desc m where

    -- | Query an indexer for the given validity interval
    query :: QueryValidity (Point desc) -> Query desc -> indexer desc -> m (Result desc)


-- | The indexer can take a result and complete it with its events
class ResumableResult indexer desc m where

    resumeResult :: Result desc -> indexer desc -> m (Result desc)


-- | The indexer can be reset to a previous `Point`
class Rewindable indexer desc m where

    rewind :: Ord (Point desc) => Point desc -> indexer desc -> m (Maybe (indexer desc))

-- | The indexer can aggregate old data.
-- The main purpose is to speed up query processing.
-- If the indexer is 'Rewindable', 'Aggregable' can compromise 'rewind' behind the 'aggregation point',
-- the idea is to call 'aggregate' on points that can't be rollbacked anymore.
class Aggregable indexer desc m where

    aggregate :: Point desc -> indexer desc -> m (indexer desc)

    aggregationPoint :: indexer desc -> m (Point desc)

-- | Points from which we can restract safely
class Resumable indexer desc m where

    -- | Last sync of the indexer
    syncPoints :: Ord (Point desc) => indexer desc -> m [Point desc]


-- | Full in memory indexer, it uses list because I was too lazy to port the 'Vector' implementation.
-- If we wanna move to these indexer, we should switch the implementation to the 'Vector' one.
data InMemory desc = InMemory
  { _events :: ![(Point desc, Event desc)] -- ^ Stored 'Event', associated with their history 'Point'
  , _latest :: !(Point desc) -- ^ Ease access to the latest datapoint
  }

makeLenses 'InMemory

instance (Monad m) => IsIndex InMemory desc m where

    insert p e ix = pure $ ix
        & events %~ ((p, e):)
        & latest .~ p

    lastSyncPoint = pure . view latest

instance Applicative m => Rewindable InMemory desc m where

    rewind p ix = pure . pure
        $ if isIndexBeforeRollback ix
             then ix
             else ix
                & events %~ dropWhile isEventAfterRollback
                & latest .~ p
      where
        isIndexBeforeRollback :: InMemory desc -> Bool
        isIndexBeforeRollback x = p >= x ^. latest
        isEventAfterRollback :: (Point desc, a) -> Bool
        isEventAfterRollback = (p <) . fst

instance Applicative m => Resumable InMemory desc m where

    syncPoints ix = let
      points = fst <$> (ix ^. events)
      addLatestIfNeeded p []        = [p]
      addLatestIfNeeded p ps@(p':_) = if p == p' then ps else p:ps
      in pure $ addLatestIfNeeded (ix ^. latest) points


newtype InDatabase desc = InDatabase { _con :: SQL.Connection }

makeLenses 'InDatabase

-- | An indexer that has at most '_slotsInMemory' events in memory and put the older one in database.
-- The query interface for this indexer will alwys go through the database first and then aggregate
-- results presents in memory.
data MixedIndexer mem store desc = MixedIndexer
    { _slotsInMemory :: !Word -- ^ How many slots do we keep in memory
    , _inMemory      :: !(mem desc) -- ^ The fast storage for latest elements
    , _inDatabase    :: !(store desc) -- ^ In database storage, should be similar to the original indexer
    }

makeLenses 'MixedIndexer


-- | Flush the in-memory events to the database, keeping track of the latest index
flush ::
    ( Monad m
    , IsIndex store desc m
    , Eq (Point desc)
    ) => MixedIndexer InMemory store desc ->
    m (MixedIndexer InMemory store desc)
flush indexer = let
    (eventsToFlush, indexer') = indexer & inMemory . events <<.~ []
    in inDatabase (insertAll eventsToFlush) indexer'

instance
    ( Monad m
    , IsIndex InMemory desc m
    , IsIndex store desc m
    ) => IsIndex (MixedIndexer InMemory store) desc m where

    insert point e indexer = do
        let maxMemSize = fromIntegral $ indexer ^. slotsInMemory
            currentSize = length (indexer ^. inMemory . events)
        if currentSize >= maxMemSize
           then do
             indexer' <- flush indexer
             inMemory (insert point e) indexer'
           else inMemory (insert point e) indexer

    lastSyncPoint = lastSyncPoint . view inMemory

instance
    ( Monad m
    , Rewindable store desc m
    ) => Rewindable (MixedIndexer InMemory store) desc m where

    rewind p indexer = do
        mindexer <-  runMaybeT $ inMemory rewindInStore indexer
        case mindexer of
          Just ix -> if null $ ix ^. inMemory . events
            then runMaybeT $ inDatabase rewindInStore ix
            else pure $ pure ix
          Nothing -> pure Nothing
      where
        rewindInStore :: Rewindable index desc m =>
          index desc -> MaybeT m (index desc)
        rewindInStore = MaybeT . rewind p

instance
    ( Monad m
    , ResumableResult InMemory desc m
    , Queryable store desc m
    ) => Queryable (MixedIndexer InMemory store) desc m where

    query valid q indexer = do
        res <- query valid q $ indexer ^. inDatabase
        resumeResult res $ indexer ^. inMemory

-- | A runner encapsulate an indexer in an opaque type, that allows to plug different indexers to the same stream of
-- input data
data RunnerM m point input =
    forall indexer desc.
    (IsIndex indexer desc m, Resumable indexer desc m, Point desc ~ point) =>
    Runner
        { runnerState  :: !(TMVar (indexer desc))
        , extractEvent :: !(input -> m (Maybe (Point desc, Event desc)))
        }

makeLenses 'Runner

type Runner = RunnerM IO

-- | create a runner for an indexer, retuning the runner and the 'MVar' it's using internally
createRunner ::
  (IsIndex indexer desc IO, Resumable indexer desc IO, point ~ Point desc) =>
  indexer desc ->
  (input -> IO (Maybe (point, Event desc))) ->
  IO (TMVar (indexer desc), Runner point input)
createRunner ix f = do
  mvar <- STM.atomically $ STM.newTMVar ix
  pure (mvar, Runner mvar f)

startRunner :: Ord point => TChan input -> QSemN -> Runner point input -> IO ()
startRunner chan tokens (Runner ix extractEvent) = do
    chan' <- STM.atomically $ STM.dupTChan chan
    forever $ do
        lockCoordinator
        me <- generateEvent extractEvent chan'
        indexGeneratedEvent me

    where

      lockCoordinator = Con.signalQSemN tokens 1

      generateEvent extract chan' = do
        x <- STM.atomically $ STM.readTChan chan'
        extract x

      indexEvent p e = do
        indexer <- STM.atomically $ STM.takeTMVar ix
        indexerLastPoint <- lastSyncPoint indexer
        if indexerLastPoint < p
           then do
             indexer' <- insert p e indexer
             STM.atomically $ STM.putTMVar ix indexer'
           else STM.atomically $ STM.putTMVar ix indexer

      indexGeneratedEvent me =
        maybe (pure ()) (uncurry indexEvent) me

data Coordinator input = Coordinator
  { tokens  :: !QSemN
  , channel :: !(TChan input)
  , runners :: !Int
  }

runnerSyncPoints :: Ord point => [Runner point input] -> IO [point]
runnerSyncPoints [] = pure []
runnerSyncPoints (r:rs) = do
    ps <- getSyncPoints r
    foldlM (\acc r' -> intersect acc <$> getSyncPoints r') ps rs

    where

      getSyncPoints :: Ord point => Runner point input -> IO [point]
      getSyncPoints (Runner ix _) = do
        indexer <- STM.atomically $ STM.takeTMVar ix
        res <- syncPoints indexer
        STM.atomically $ STM.putTMVar ix indexer
        pure res


start :: Ord point => [Runner point input] -> IO (Coordinator input)
start rs = do
    let nb = length rs
    tokens <- STM.newQSemN 0
    channel <- STM.newBroadcastTChanIO
    startRunners channel tokens
    pure $ Coordinator tokens channel nb
    where
      startRunners channel tokens =
          traverse_ (startRunner channel tokens) rs

step :: Coordinator input -> input -> IO ()
step c i = do
    waitRunners
    dispatchNewInput

    where

      waitRunners =
        Con.waitQSemN (tokens c) (runners c)

      dispatchNewInput =
        STM.atomically $ STM.writeTChan (channel c) i

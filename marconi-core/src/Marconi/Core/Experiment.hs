{- |
 This module propose an alternative to the index implementation proposed in 'RewindableIndex.Storable'.

 The point we wanted to address are the folowing:

   - 'Storable' implementation is designed in a way that strongly promote indexer that rely on a mix of database and
     in-memory storage. We try to propose a more generic design that would allow

        - full in-memory indexers
        - indexer backed by a simple file
        - mock indexer, for testing purpose, with predefined behaviour
        - group of indexers, synchronised as a single indexer
        - implement in-memory/database storage that rely on other query heuristic

   - We want to be able to compose easily indexers to build new ones. For example, the original indexer design can be
     seen as the combination of two indexers, a full in-memory indexer, and a full in database indexer.

   - The original implementation considered the 'StorablePoint' as a data that can be derived from 'Event',
     leading to the design of synthetic events to deal with indexer that didn't index enough data.

   - In marconi, the original design use an exotic callback design to handle `MVar` modification,
     we wanted to address this point as well.

What's include in this module:

   - Base type classes to define an indexer, its query interface, and the required plumbing to handle rollback
   - A full in-memory indexer (naive), an indexer that compose it with a SQL layer for persistence
   - A coordinator for indexers, that can be exposed as an itdexer itself

-}
module Marconi.Core.Experiment where

import Control.Concurrent (QSemN)
import Control.Concurrent qualified as Con
import Control.Lens (makeLenses, view, (%~), (&), (<<.~), (?~), (^.))
import Control.Monad (forever, guard)
import Control.Tracer (Tracer, traceWith)

import Control.Concurrent qualified as STM
import Control.Concurrent.STM (TChan, TMVar)
import Control.Concurrent.STM qualified as STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Foldable (foldlM, foldrM, traverse_)
import Data.Functor (($>))
import Data.List (intersect)
import Database.SQLite.Simple qualified as SQL

-- | A point in time
type family Point desc
-- |
-- A an element that you want to capture from a given input. A given point in time will always correspond to an event.
-- As a consequence if a point in time can be associated with no event, wrap a `Maybe` type, if several events need to be
-- associated to the same point in time, wrap a `List`.
type family Event desc
data family Result query


class Monad m => IsIndex indexer desc m where

    insert :: Eq (Point desc) =>
        Point desc -> Event desc -> indexer desc -> m (indexer desc)

    -- | Store a bunch of points, associated to their event, in an indexer
    insertAll :: (Eq (Point desc), Foldable f) =>
        f (Point desc, Event desc) -> indexer desc -> m (indexer desc)
    insertAll = flip (foldrM (uncurry insert))

    -- | Last sync of the indexer
    lastSyncPoint :: indexer desc -> m (Maybe (Point desc))

-- | The indexer can answer a Query to produce the corresponding result
class Queryable indexer desc query m where

    -- | Query an indexer at a given point in time
    query :: Point desc -> query -> indexer desc -> m (Result query)


-- | The indexer can take a result and complete it with its events
class ResumableResult indexer desc result m where

    resumeResult :: result -> indexer desc -> m result


-- | The indexer can be reset to a previous `Point`
class Rewindable indexer desc m where

    rewind :: Ord (Point desc) => Point desc -> indexer desc -> m (Maybe (indexer desc))

-- | The indexer can aggregate old data.
-- The main purpose is to speed up query processing.
-- If the indexer is 'Rewindable', 'Aggregable' can't 'rewind' behind the 'aggregationPoint',
-- the idea is to call 'aggregate' on points that can't be rollbacked anymore.
class Aggregable indexer desc m where

    -- Aggregate events of the indexer up to a given point in time
    aggregate :: Point desc -> indexer desc -> m (indexer desc)

    -- The latest aggregation point (aggregation up to the result are aggregated)
    aggregationPoint :: indexer desc -> m (Point desc)

-- | Points from which we can restract safely
class Resumable indexer desc m where

    -- | Last sync of the indexer
    syncPoints :: Ord (Point desc) => indexer desc -> m [Point desc]


-- | Full in memory indexer, it uses list because I was too lazy to port the 'Vector' implementation.
-- If we wanna move to these indexer, we should switch the implementation to the 'Vector' one.
data InMemory desc = InMemory
  { _events :: ![(Point desc, Event desc)] -- ^ Stored 'Event', associated with their history 'Point'
  , _latest :: !(Maybe (Point desc)) -- ^ Ease access to the latest datapoint
  }

makeLenses 'InMemory

instance (Monad m) => IsIndex InMemory desc m where

    insert p e ix = pure $ ix
        & events %~ ((p, e):)
        & latest ?~ p

    lastSyncPoint = pure . view latest

instance Applicative m => Rewindable InMemory desc m where

    rewind p ix = pure . pure
        $ if isIndexBeforeRollback ix
             then ix
             else ix
                & cleanRecentEvents
                & adjustLatestPoint
      where
        adjustLatestPoint :: InMemory desc -> InMemory desc
        adjustLatestPoint = latest ?~ p
        cleanRecentEvents :: InMemory desc -> InMemory desc
        cleanRecentEvents = events %~ dropWhile isEventAfterRollback
        isIndexBeforeRollback :: InMemory desc -> Bool
        isIndexBeforeRollback x = maybe True (p >=) $ x ^. latest
        isEventAfterRollback :: (Point desc, a) -> Bool
        isEventAfterRollback = (p <) . fst

instance Applicative m => Resumable InMemory desc m where

    syncPoints ix = let
      indexPoints = fst <$> (ix ^. events)
      -- if the latest point of the index is not a stored event, we add it to the list of points
      addLatestIfNeeded Nothing ps         = ps
      addLatestIfNeeded (Just p) []        = [p]
      addLatestIfNeeded (Just p) ps@(p':_) = if p == p' then ps else p:ps
      in pure $ addLatestIfNeeded (ix ^. latest) indexPoints


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
    flushMemory = inMemory . events <<.~ []
    (eventsToFlush, indexer') = flushMemory indexer
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
            else pure $ pure ix -- if there are still event in memory, no need to rewind the database
          Nothing -> pure Nothing
      where
        rewindInStore :: Rewindable index desc m => index desc -> MaybeT m (index desc)
        rewindInStore = MaybeT . rewind p

instance
    ( Monad m , ResumableResult InMemory desc (Result query) m , Queryable store desc query m) =>
    Queryable (MixedIndexer InMemory store) desc query m where

    query valid q indexer = do
        res <- query valid q $ indexer ^. inDatabase
        resumeResult res $ indexer ^. inMemory

data IndexerNotification desc
   = Rollback !(Point desc)
   | Process !(Point desc)
   | Issue !(Event desc)

-- | A runner encapsulate an indexer in an opaque type, that allows to plug different indexers to the same stream of
-- input data
data RunnerM m input point =
    forall indexer desc.
    (IsIndex indexer desc m, Resumable indexer desc m, Rewindable indexer desc m, Point desc ~ point) =>
    Runner
        { runnerState      :: !(TMVar (indexer desc))
        , identifyRollback :: !(input -> m (Maybe (Point desc)))
        , extractEvent     :: !(input -> m (Maybe (Point desc, Event desc)))
        , tracer           :: !(Tracer m (IndexerNotification desc))
        }

makeLenses 'Runner

type Runner = RunnerM IO

-- | create a runner for an indexer, retuning the runner and the 'MVar' it's using internally
createRunner ::
  (IsIndex indexer desc IO, Resumable indexer desc IO, Rewindable indexer desc IO, point ~ Point desc) =>
  indexer desc ->
  (input -> IO (Maybe point)) ->
  (input -> IO (Maybe (point, Event desc))) ->
  Tracer IO (IndexerNotification desc) ->
  IO (TMVar (indexer desc), Runner input point)
createRunner ix rb f tr = do
  mvar <- STM.atomically $ STM.newTMVar ix
  pure (mvar, Runner mvar rb f tr)

-- | The runner start waiting fo new event and process them as they come
startRunner :: Ord point => TChan input -> QSemN -> Runner input point -> IO ()
startRunner chan tokens (Runner ix isRollback extractEvent tracer) = do
    chan' <- STM.atomically $ STM.dupTChan chan
    input <- STM.atomically $ STM.readTChan chan'
    forever $ do
        unlockCoordinator
        rollBackPoint <- isRollback input
        maybe (handleInsert input) handleRollback rollBackPoint

    where

      unlockCoordinator = Con.signalQSemN tokens 1

      indexEvent p e = do
          indexer <- STM.atomically $ STM.takeTMVar ix
          indexerLastPoint <- lastSyncPoint indexer
          if maybe True (< p) indexerLastPoint
             then do
                 indexer' <- insert p e indexer
                 STM.atomically $ STM.putTMVar ix indexer'
             else STM.atomically $ STM.putTMVar ix indexer

      handleInsert input = do
          me <- extractEvent input
          case me of
               Nothing -> pure ()
               Just (point, event) -> do
                   traceWith tracer (Issue event)
                   indexEvent point event

      handleRollback p = do
          indexer <- STM.atomically $ STM.takeTMVar ix
          mindexer <- rewind p indexer
          maybe
              (STM.atomically $ STM.putTMVar ix indexer)
              (STM.atomically . STM.putTMVar ix)
              mindexer
          traceWith tracer (Rollback p)

data Coordinator input point = Coordinator
  { _lastSync  :: !(Maybe point) -- ^ the last common sync point for the runners
  , _runners   :: ![Runner input point] -- ^ the list of runners managed by this coordinator
  , _tokens    :: !QSemN -- ^ use to synchronise the runner
  , _channel   :: !(TChan input) -- ^ to dispatch input to runners
  , _nbRunners :: !Int -- ^ how many runners are we waiting for, should always be equal to @length runners@
  }

makeLenses 'Coordinator

-- | Get the common syncPoints of a group or runners
runnerSyncPoints :: Ord point => [Runner input point] -> IO [point]
runnerSyncPoints [] = pure []
runnerSyncPoints (r:rs) = do
    ps <- getSyncPoints r
    foldlM (\acc r' -> intersect acc <$> getSyncPoints r') ps rs

    where

        getSyncPoints :: Ord point => Runner input point -> IO [point]
        getSyncPoints (Runner ix _ _ _) = do
            indexer <- STM.atomically $ STM.takeTMVar ix
            res <- syncPoints indexer
            STM.atomically $ STM.putTMVar ix indexer
            pure res

-- | create a coordinator with started runners
start :: Ord point => [Runner input point] -> IO (Coordinator input point)
start runners' = do
    let nb = length runners'
    tokens' <- STM.newQSemN 0 -- starts empty, will be filled when the runners will start
    channel' <- STM.newBroadcastTChanIO
    startRunners channel' tokens'
    pure $ Coordinator Nothing runners' tokens' channel' nb
    where
        startRunners channel' tokens' = traverse_ (startRunner channel' tokens') runners'

-- A coordinator step (send an input, wait for an ack of every runner that it's processed)
step :: (input -> point) -> Coordinator input point -> input -> IO (Coordinator input point)
step getPoint coordinator input = do
    dispatchNewInput
    waitRunners $> setLastSync coordinator
    where
      waitRunners = Con.waitQSemN (coordinator ^. tokens) (coordinator ^. nbRunners)
      dispatchNewInput = STM.atomically $ STM.writeTChan (coordinator ^. channel) input
      setLastSync c = c & lastSync ?~ getPoint input


-- A coordinator can be seen as an indexer
newtype CoordinatorIndex desc =
     CoordinatorIndex
          { _coordinator :: Coordinator (Event desc) (Point desc)
          }

makeLenses 'CoordinatorIndex

-- A coordinator can be consider as an indexer that forwards the input to its runner
instance IsIndex CoordinatorIndex desc IO where

    insert point event = coordinator $ \x -> step (const point) x event

    lastSyncPoint indexer = pure $ indexer ^. coordinator . lastSync

-- | To rewind a coordinator, we try and rewind all the runners.
instance Rewindable CoordinatorIndex desc IO where

    rewind p = runMaybeT . coordinator rewindRunners
        where
            rewindRunners ::
                Coordinator (Event desc) (Point desc) ->
                MaybeT IO (Coordinator (Event desc) (Point desc))
            rewindRunners c = do
                availableSyncs <- lift $ runnerSyncPoints $ c ^. runners
                guard $ p `elem` availableSyncs -- we start by checking if the given point is a valid sync point
                runners (traverse $ MaybeT . rewindRunner) c

            rewindRunner ::
                Runner (Event desc) (Point desc) ->
                IO (Maybe (Runner (Event desc) (Point desc)))
            rewindRunner r@Runner{runnerState} = do
                indexer <- STM.atomically $ STM.takeTMVar runnerState
                res <- rewind p indexer
                maybe
                    -- the Nothing case should not happen as we check that the sync point is a valid one
                    (STM.atomically (STM.putTMVar runnerState indexer) $> Nothing)
                    ((Just r <$) . STM.atomically . STM.putTMVar runnerState)
                    res

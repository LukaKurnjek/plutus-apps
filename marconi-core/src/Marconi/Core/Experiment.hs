{- |
 This module propose an alternative to the index implementation proposed in 'RewindableIndex.Storable'.

 The point we wanted to address are the folowing:

   - 'Storable' implementation is designed in a way that strongly promote indexer that rely on a mix of database and
     in-memory storage. We try to propose a more generic design that would allow

        - full in-memory indexers
        - indexer backed by a simple file
        - indexer transformers, that add capability (logging, caching...) to an indexer
        - mock indexer, for testing purpose, with predefined behaviour
        - group of indexers, synchronised as a single indexer
        - implement in-memory/database storage that rely on other query heuristic

   - We want to be able to compose easily indexers to build new ones. For example, the original indexer design can be
     seen as the combination of two indexers, a full in-memory indexer, and a full in database indexer.

   - The original implementation considered the 'StorablePoint' as data that can be derived from 'Event',
     leading to the design of synthetic events to deal with indexer that didn't index enough data.

   - In marconi, the original design uses a callback design to handle `MVar` modification,
     we wanted to address this point as well.

What's included in this module:

   - Base type classes to define an indexer, its query interface, and the required plumbing to handle rollback
   - A full in-memory indexer (naive), an indexer that compose it with a SQL layer for persistence
   - Tracing, as a modifier to an existing indexer (it allows us to opt-in for traces if we want, indexer by indexer)
   - A coordinator for indexers, that can be exposed as an itdexer itself
   - Some queries that can be applied to many indexers

  Contrary to the original Marconi design, indexers don't have a unique (in-memory/sqlite) implementation.

-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE UndecidableInstances #-}
module Marconi.Core.Experiment where

import Control.Concurrent (QSemN)
import Control.Concurrent qualified as Con
import Control.Lens (filtered, folded, makeLenses, view, (%~), (&), (<<.~), (?~), (^.), (^..), (^?))
import Control.Monad (forever, guard)
import Control.Monad.Except (MonadError (catchError, throwError))
import Control.Tracer (Tracer, traceWith)

import Control.Concurrent qualified as STM
import Control.Concurrent.STM (TChan, TMVar)
import Control.Concurrent.STM qualified as STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Foldable (foldlM, foldrM, traverse_)
import Data.Functor (($>))
import Data.List (intersect)
import Data.Text (Text)


-- * Indexer Types

-- | A point in time, the concrete type of a point is now derived from an indexer event,
-- instead of an event.
-- The reason is that you may not want to always carry around a point when you manipulate an event.
type family Point event

-- | A result is a data family from the corresponding query descriptor.
-- A query is tied to an indexer by a typeclass, this design choice has two main reasons:
--     * we want to be able to define different query for the same indexer
--       (eg. we may want to define two distinct query types for an utxo indexer:
--       one to ge all the utxo for a given address,
--       another one for to get all the utxos emitted at a given slot).
--     * we want to assign a query type to different indexers.
data family Result query


-- | Attach an event to a point in time
data TimedEvent event =
     TimedEvent
         { _point :: !(Point event)
         , _event :: !event
         }

makeLenses 'TimedEvent

-- | The base class of an indexer.
-- The indexer should provide two main functionality: indexing events, and providing its last synchronisation point.
--
-- @indexer@ the indexer implementation type
-- @event@ the indexed events
-- @m@ the monad in which our indexer operates
class Monad m => IsIndex indexer event m where

    -- | index an event at a given point in time
    index :: Eq (Point event) =>
        TimedEvent event -> indexer event -> m (indexer event)

    -- | Index a bunch of points, associated to their event, in an indexer
    indexAll :: (Eq (Point event), Foldable f) =>
        f (TimedEvent event) -> indexer event -> m (indexer event)
    indexAll = flip (foldrM index)

    -- | Last sync of the indexer
    lastSyncPoint :: indexer event -> m (Maybe (Point event))
    {-# MINIMAL index, lastSyncPoint #-}

-- | Check if the given point is ahead of the last syncPoint of an indexer,
isNotAheadOfSync ::
    (Ord (Point event), IsIndex indexer event m) =>
    Point event -> indexer event -> m Bool
isNotAheadOfSync p indexer = maybe False (p <=) <$> lastSyncPoint indexer


-- | Error that can occurs when you query an indexer
data QueryError query
   = AheadOfLastSync !(Maybe (Result query))
     -- ^ The required point is ahead of the current index.
     -- The error may still provide its latest result if it make sense for the given query.
     --
     -- It can be useful for indexer that contains a partial knowledge and that want to pass
     -- this knowledge to another indexer to complete the query.
   | NotStoredAnymore
     -- ^ The requested point is too far in the past and has been aggregated
   | IndexerError !Text
     -- ^ The indexer query failed

-- | The indexer can answer a Query to produce the corresponding result of that query.
--
--     * @indexer@ is the indexer implementation type
--     * @event@ the indexer events
--     * @query@ the type of query we want to answer
--     * @m@ the monad in which our indexer operates
class MonadError (QueryError query) m => Queryable indexer event query m where

    -- | Query an indexer at a given point in time
    -- It can be read as:
    -- "With the knowledge you have at that point in time,
    --  what is your answer to this query?"
    query :: Ord (Point event) => Point event -> query -> indexer event -> m (Result query)

-- | We can reset an indexer to a previous `Point`
--     * @indexer@ is the indexer implementation type
--     * @event@ the indexer events
--     * @m@ the monad in which our indexer operates
class Rewindable indexer event m where

    rewind :: Ord (Point event) => Point event -> indexer event -> m (Maybe (indexer event))

-- | The indexer can aggregate old data.
-- The main purpose is to speed up query processing.
-- If the indexer is 'Rewindable', 'Aggregable' can't 'rewind' behind the 'aggregationPoint',
-- the idea is to call 'aggregate' on points that can't be rollbacked anymore.
--
--     * @indexer@ is the indexer implementation type
--     * @desc@ the descriptor of the indexer, fixing the @Point@ types
--     * @m@ the monad in which our indexer operates
class Aggregable indexer event m where

    -- Aggregate events of the indexer up to a given point in time
    aggregate :: Point event -> indexer event -> m (indexer event)

    -- The latest aggregation point (aggregation up to the result are aggregated)
    aggregationPoint :: indexer event -> m (Point event)

-- | Points from which we can restract safely
class Resumable indexer event m where

    -- | List the points that we still have in the indexers, allowing us to restart from them
    syncPoints :: Ord (Point event) => indexer event -> m [Point event]


-- * Base runIndexers

-- ** Full in-memory indexer, backed by a list

-- | A Full in memory indexer, it uses list because I was too lazy to port the 'Vector' implementation.
-- If we wanna move to these indexer, we should switch the implementation to the 'Vector' one.
data InMemory event = InMemory
  { _events :: ![TimedEvent event] -- ^ Stored 'Event', associated with their history 'Point'
  , _latest :: !(Maybe (Point event)) -- ^ Ease access to the latest datapoint
  }

makeLenses 'InMemory

instance (Monad m) => IsIndex InMemory event m where

    index timedEvent ix = let
        appendEvent = events %~ (timedEvent:)
        updateLatest = latest ?~ (timedEvent ^. point)
        in pure $ ix
            & appendEvent
            & updateLatest

    lastSyncPoint = pure . view latest

instance Applicative m => Rewindable InMemory event m where

    rewind p ix = pure . pure
        $ if isIndexBeforeRollback ix
             then ix -- if we're already before the rollback, we don't have to do anything
             else ix
                & cleanEventsAfterRollback
                & adjustLatestPoint
      where
        adjustLatestPoint :: InMemory event -> InMemory event
        adjustLatestPoint = latest ?~ p
        cleanEventsAfterRollback :: InMemory event -> InMemory event
        cleanEventsAfterRollback = events %~ dropWhile isEventAfterRollback
        isIndexBeforeRollback :: InMemory event -> Bool
        isIndexBeforeRollback x = maybe True (p >=) $ x ^. latest
        isEventAfterRollback :: TimedEvent event -> Bool
        isEventAfterRollback = (p <) . view point

instance Applicative m => Resumable InMemory event m where

    syncPoints ix = let
      indexPoints = ix ^.. events . folded . point
      -- if the latest point of the index is not a stored event, we add it to the list of points
      addLatestIfNeeded Nothing ps         = ps
      addLatestIfNeeded (Just p) []        = [p]
      addLatestIfNeeded (Just p) ps@(p':_) = if p == p' then ps else p:ps
      in pure $ addLatestIfNeeded (ix ^. latest) indexPoints


-- ** Mixed indexer

-- | An indexer that has at most '_blocksInMemory' events in memory and put the older one in database.
-- The query interface for this indexer will alwyas go through the database first and then aggregate
-- results present in memory.
--
-- @mem@ the indexer that handle old events, when we need to remove stuff from memory
-- @store@ the indexer that handle the most recent events
data MixedIndexer mem store event = MixedIndexer
    { _blocksInMemory :: !Word -- ^ How many blocks do we keep in memory
    , _inMemory       :: !(mem event) -- ^ The fast storage for latest elements
    , _inDatabase     :: !(store event) -- ^ In database storage, should be similar to the original indexer
    }

makeLenses 'MixedIndexer

-- | The indexer can take a result and complete it with its events
-- It's used by the in-memory part of a 'MixedIndexer' to specify
-- how we can complete the database result with the memory content.
class ResumableResult indexer event query m where

    resumeResult ::
       Ord (Point event) =>
       Point event -> query -> indexer event -> m (Result query) -> m (Result query)



-- | Flush all the in-memory events to the database, keeping track of the latest index
flush ::
    ( Monad m
    , IsIndex store event m
    , Eq (Point event)
    ) => MixedIndexer InMemory store event ->
    m (MixedIndexer InMemory store event)
flush indexer = let
    flushMemory = inMemory . events <<.~ []
    (eventsToFlush, indexer') = flushMemory indexer
    in inDatabase (indexAll eventsToFlush) indexer'

instance
    ( Monad m
    , IsIndex InMemory event m
    , IsIndex store event m
    ) => IsIndex (MixedIndexer InMemory store) event m where

    index timedEvent indexer = do
        let maxMemSize = fromIntegral $ indexer ^. blocksInMemory
            currentSize = length (indexer ^. inMemory . events)
        if currentSize >= maxMemSize
           then do
             indexer' <- flush indexer
             inMemory (index timedEvent) indexer'
           else inMemory (index timedEvent) indexer

    lastSyncPoint = lastSyncPoint . view inMemory

instance
    ( Monad m
    , Rewindable store event m
    ) => Rewindable (MixedIndexer InMemory store) event m where

    rewind p indexer = do
        mindexer <-  runMaybeT $ inMemory rewindInStore indexer
        case mindexer of
          Just ix -> if not $ null $ ix ^. inMemory . events
            then pure $ pure ix -- if there are still events in memory, no need to rewind the database
            else runMaybeT $ inDatabase rewindInStore ix
          Nothing -> pure Nothing
      where
        rewindInStore :: Rewindable index event m => index event -> MaybeT m (index event)
        rewindInStore = MaybeT . rewind p

instance
    ( ResumableResult InMemory event query m
    , Queryable store event query m
    ) =>
    Queryable (MixedIndexer InMemory store) event query m where

    query valid q indexer = resumeResult valid q (indexer ^. inMemory) (query valid q (indexer ^. inDatabase))


-- * Indexer transformer: Add effects

-- ** Tracer Add tracing to an existing indexer

-- | Remarkable events of an indexer
data IndexerNotification event
   = Rollback !(Point event)
   | Index !(TimedEvent event)

-- | A tracer modifier that adds tracing to an existing indexer
data WithTracer m indexer event =
     WithTracer
         { _tracedIndexer :: !(indexer event)
         , _tracer        :: !(Tracer m (IndexerNotification event))
         }

makeLenses 'WithTracer

instance
    (Monad m, IsIndex index event m) =>
    IsIndex (WithTracer m index) event m where

    index timedEvent indexer = do
        res <- tracedIndexer (index timedEvent) indexer
        traceWith (indexer ^. tracer) $ Index timedEvent
        pure res

    lastSyncPoint = lastSyncPoint . view tracedIndexer

instance
    ( Monad m
    , Rewindable index event m
    ) => Rewindable (WithTracer m index) event m where

    rewind p indexer = do
      res <- runMaybeT $ rewindWrappedIndexer p
      maybe (pure Nothing) traceSuccessfulRewind res
     where
         rewindWrappedIndexer p' = tracedIndexer (MaybeT . rewind p') indexer
         traceSuccessfulRewind indexer' = do
              traceWith (indexer' ^. tracer) (Rollback p)
              pure $ Just indexer'


-- ** Runners

-- | A runner encapsulate an indexer in an opaque type.
-- It allows us to manipulate seamlessly a list of indexers that has different types
-- as long as they implement the required interfaces.
data RunnerM m input point =
    forall indexer event.
    ( IsIndex indexer event m
    , Resumable indexer event m
    , Rewindable indexer event m
    , Point event ~ point
    ) =>
    Runner
        { runnerState      :: !(TMVar (indexer event))
        , identifyRollback :: !(input -> m (Maybe (Point event)))
        , extractEvent     :: !(input -> m (Maybe (TimedEvent event)))
        }

type Runner = RunnerM IO

-- | create a runner for an indexer, retuning the runner and the 'MVar' it's using internally
createRunner ::
  ( IsIndex indexer event IO
  , Resumable indexer event IO
  , Rewindable indexer event IO
  , point ~ Point event) =>
  indexer event ->
  (input -> IO (Maybe point)) ->
  (input -> IO (Maybe (TimedEvent event))) ->
  IO (TMVar (indexer event), Runner input point)
createRunner ix rb f = do
  mvar <- STM.atomically $ STM.newTMVar ix
  pure (mvar, Runner mvar rb f)

-- | The runner notify its coordinator that it's ready
-- and starts waiting for new events and process them as they come
startRunner :: Ord point => TChan input -> QSemN -> Runner input point -> IO ()
startRunner chan tokens (Runner ix isRollback extractEvent) = do
    chan' <- STM.atomically $ STM.dupTChan chan
    input <- STM.atomically $ STM.readTChan chan'
    forever $ do
        unlockCoordinator
        rollBackPoint <- isRollback input
        maybe (handleInsert input) handleRollback rollBackPoint

    where

      unlockCoordinator = Con.signalQSemN tokens 1

      fresherThan evt p = maybe True (< evt ^. point) p

      indexEvent timedEvent = do
          indexer <- STM.atomically $ STM.takeTMVar ix
          indexerLastPoint <- lastSyncPoint indexer
          if timedEvent `fresherThan` indexerLastPoint
             then do
                 indexer' <- index timedEvent indexer
                 STM.atomically $ STM.putTMVar ix indexer'
             else STM.atomically $ STM.putTMVar ix indexer

      handleInsert input = do
          me <- extractEvent input
          case me of
               Nothing -> pure ()
               Just timedEvent -> do
                   indexEvent timedEvent

      handleRollback p = do
          indexer <- STM.atomically $ STM.takeTMVar ix
          mindexer <- rewind p indexer
          maybe
              (STM.atomically $ STM.putTMVar ix indexer)
              (STM.atomically . STM.putTMVar ix)
              mindexer

-- | A coordinator synchronises the event processing of a list of indexers.
-- A coordinator is itself is an indexer.
-- It means that we can create a tree of indexer, with coordinators that partially process the data at each node,
-- and with concrete indexers at the leaves.
data Coordinator input point = Coordinator
  { _lastSync  :: !(Maybe point) -- ^ the last common sync point for the runners
  , _runners   :: ![Runner input point] -- ^ the list of runners managed by this coordinator
  , _tokens    :: !QSemN -- ^ use to synchronise the runner
  , _channel   :: !(TChan input) -- ^ to dispatch input to runners
  , _nbRunners :: !Int -- ^ how many runners are we waiting for, should always be equal to @length runners@
  }

makeLenses 'Coordinator

-- | Get the common syncPoints of a group or runners
--
-- Important note : the syncpoints are sensible to rewind. It means that the result of this function may be invalid if
-- the indexer is rewinded.
runnerSyncPoints :: Ord point => [Runner input point] -> IO [point]
runnerSyncPoints [] = pure []
runnerSyncPoints (r:rs) = do
    ps <- getSyncPoints r
    foldlM (\acc r' -> intersect acc <$> getSyncPoints r') ps rs

    where

        getSyncPoints :: Ord point => Runner input point -> IO [point]
        getSyncPoints (Runner ix _ _) = do
            indexer <- STM.atomically $ STM.readTMVar ix
            syncPoints indexer

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

-- A coordinator step (send an input to its runners, wait for an ack of every runner before listening again)
step :: (input -> point) -> Coordinator input point -> input -> IO (Coordinator input point)
step getPoint coordinator input = do
    dispatchNewInput
    waitRunners $> setLastSync coordinator
    where
      waitRunners = Con.waitQSemN (coordinator ^. tokens) (coordinator ^. nbRunners)
      dispatchNewInput = STM.atomically $ STM.writeTChan (coordinator ^. channel) input
      setLastSync c = c & lastSync ?~ getPoint input


-- A coordinator can be seen as an indexer
newtype CoordinatorIndex event =
     CoordinatorIndex
          { _coordinator :: Coordinator event (Point event)
          }

makeLenses 'CoordinatorIndex

-- A coordinator can be consider as an indexer that forwards the input to its runner
instance IsIndex CoordinatorIndex event IO where

    index timedEvent = coordinator $
            \x -> step (const $ timedEvent ^. point) x $ timedEvent ^. event

    lastSyncPoint indexer = pure $ indexer ^. coordinator . lastSync

-- | To rewind a coordinator, we try and rewind all the runners.
instance Rewindable CoordinatorIndex event IO where

    rewind p = runMaybeT . coordinator rewindRunners
        where
            rewindRunners ::
                Coordinator event (Point event) ->
                MaybeT IO (Coordinator event (Point event))
            rewindRunners c = do
                availableSyncs <- lift $ runnerSyncPoints $ c ^. runners
                -- we start by checking if the given point is a valid sync point
                guard $ p `elem` availableSyncs
                runners (traverse $ MaybeT . rewindRunner) c

            rewindRunner ::
                Runner event (Point event) ->
                IO (Maybe (Runner event (Point event)))
            rewindRunner r@Runner{runnerState} = do
                indexer <- STM.atomically $ STM.takeTMVar runnerState
                res <- rewind p indexer
                maybe
                    -- the Nothing case should not happen
                    -- as we check that the sync point is a valid one
                    (STM.atomically (STM.putTMVar runnerState indexer) $> Nothing)
                    ((Just r <$) . STM.atomically . STM.putTMVar runnerState)
                    res

-- There is no point in providing a 'Queryable' interface for 'CoordinatorIndex' though,
-- as it's sole interest would be to get the latest synchronisation points,
-- but 'query' requires a 'Point' to provide a result.


-- * Common query interfaces

-- ** Get Event at a given point in time

-- | Get the event stored by the indexer at a given point in time
data EventAtQuery event = EventAtQuery

-- | The result of EventAtQuery is always an event.
-- The error cases are handled by the query interface.
-- in time
newtype instance Result (EventAtQuery event) =
    EventAtResult {getEvent :: event}

instance MonadError (QueryError (EventAtQuery event)) m =>
    Queryable InMemory event (EventAtQuery event) m where

    query p EventAtQuery ix = do
        let isAtPoint e p' = e ^. point == p'
        check <- isNotAheadOfSync p ix
        if check
        then maybe
             -- If we can't find the point and if it's in the past, we probably moved it
            (throwError NotStoredAnymore)
            (pure . EventAtResult)
            $ ix ^? events . folded . filtered (`isAtPoint` p) . event
        else throwError $ AheadOfLastSync Nothing

instance MonadError (QueryError (EventAtQuery event)) m =>
    ResumableResult InMemory event (EventAtQuery event) m where

    resumeResult p q indexer result = result `catchError` \case
         -- If we didn't find a result in the 1st indexer, try in memory
        _inDatabaseError -> query p q indexer

-- ** Filtering available events

-- | Query an indexer to find all events that match a given predicate
newtype EventsMatchingQuery event = EventsMatchingQuery {predicate :: event -> Bool}

-- | The result of an @EventMatchingQuery@
newtype instance Result (EventsMatchingQuery event) = EventsMatching {filteredEvents :: [event]}

deriving newtype instance Semigroup (Result (EventsMatchingQuery event))

instance (MonadError (QueryError (EventsMatchingQuery event)) m, Applicative m) =>
    Queryable InMemory event (EventsMatchingQuery event) m where

    query p q ix = do
        let isBefore e p' = e ^. point <= p'
        let result = EventsMatching $ ix ^.. events . folded . filtered (`isBefore` p) . event . filtered (predicate q)
        check <- isNotAheadOfSync p ix
        if check
            then pure result
            else throwError . AheadOfLastSync . Just $ result

instance MonadError (QueryError (EventsMatchingQuery event)) m =>
    ResumableResult InMemory event (EventsMatchingQuery event) m where

    resumeResult p q indexer result = result `catchError` \case
         -- If we find an incomplete result in the first indexer, complete it
        AheadOfLastSync (Just r) -> (<> r) <$> query p q indexer
        inDatabaseError          -> throwError inDatabaseError -- For any other error, forward it

{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE UndecidableInstances #-}
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
   - A coordinator for indexers, that can be exposed as an itdexer itself
   - Some queries that can be applied to many indexers
   - Several modifiers for indexers:
       - Tracing, as a modifier to an existing indexer
         (it allows us to opt-in for traces if we want, indexer by indexer)
       - Delay to delay event processing for heavy computation
       - Aggregate, to compact data that can't be rollbacked

  Contrary to the original Marconi design, indexers don't have a unique (in-memory/sqlite) implementation.

-}
module Marconi.Core.Experiment where

import Control.Concurrent qualified as Con (newQSemN, signalQSemN, waitQSemN)
import Control.Concurrent.STM qualified as STM (atomically, dupTChan, newBroadcastTChanIO, newTMVar, putTMVar,
                                                readTChan, readTMVar, takeTMVar, writeTChan)
import Control.Tracer qualified as Tracer (traceWith)
import Data.Sequence qualified as Seq

import Control.Concurrent (QSemN)
import Control.Lens (filtered, folded, makeLenses, set, view, (%~), (&), (+~), (-~), (.~), (<<.~), (?~), (^.), (^..),
                     (^?))
import Control.Monad (forever, guard, when)
import Control.Monad.Except (ExceptT, MonadError (catchError, throwError), runExceptT)
import Control.Tracer (Tracer)

import Control.Concurrent.STM (TChan, TMVar)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Bifunctor (first)
import Data.Foldable (foldlM, foldrM, traverse_)
import Data.Functor (($>))
import Data.Functor.Compose (Compose (Compose, getCompose))
import Data.List (intersect)
import Data.Sequence (Seq (Empty, (:|>)), (<|))
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

-- | Error that can occur when you index events
data IndexError indexer event
   = NoSpaceLeft !(indexer event)
     -- ^ An indexer with limited capacity is full and is unable to index an event
   | OtherError !Text
     -- ^ Any other cause of failure

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
    {-# MINIMAL index #-}

class IsSync indexer event m where

    -- | Last sync of the indexer
    lastSyncPoint :: indexer event -> m (Maybe (Point event))

-- | Check if the given point is ahead of the last syncPoint of an indexer,
isNotAheadOfSync ::
    (Ord (Point event), IsSync indexer event m, Functor m) =>
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
   | IndexerQueryError !Text
     -- ^ The indexer query failed

-- | The indexer can answer a Query to produce the corresponding result of that query.
--
--     * @indexer@ is the indexer implementation type
--     * @event@ the indexer events
--     * @query@ the type of query we want to answer
--     * @m@ the monad in which our indexer operates
class Queryable indexer event query m where

    -- | Query an indexer at a given point in time
    -- It can be read as:
    -- "With the knowledge you have at that point in time,
    --  what is your answer to this query?"
    query :: Ord (Point event) => Point event -> query -> indexer event -> m (Result query)

-- | Like @query@, but internalise @QueryError@ in the result.
query'
    :: (Queryable indexer event query (ExceptT (QueryError query) m), Ord (Point event))
    => Point event -> query -> indexer event -> m (Either (QueryError query) (Result query))
query' p q = runExceptT . query p q

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


-- * Base indexers

-- ** Full in-memory indexer

type family Container (indexer :: * -> *) :: * -> *

-- | Define an in-memory container with a limited memory
class BoundedMemory indexer m where

    -- | Check if there isn't space left in memory
    isFull  :: indexer event -> m Bool

    -- | Clear the memory and return its content
    flushMemory :: indexer event -> m (Container indexer (TimedEvent event), indexer event)

-- | A Full in memory indexer, it uses list because I was too lazy to port the 'Vector' implementation.
-- If we wanna move to these indexer, we should switch the implementation to the 'Vector' one.
data ListIndexer event =
    ListIndexer
    { _capacity :: !Word
    , _events   :: ![TimedEvent event] -- ^ Stored 'Event', associated with their history 'Point'
    , _latest   :: !(Maybe (Point event)) -- ^ Ease access to the latest datapoint
    }

type instance Container ListIndexer = []

makeLenses 'ListIndexer

instance Applicative m => BoundedMemory ListIndexer m where

    isFull ix = pure $ ix ^. capacity >= fromIntegral (length (ix ^. events))

    flushMemory ix = pure $ ix & events <<.~ []

instance
    (MonadError (IndexError ListIndexer event) m, Monad m) =>
    IsIndex ListIndexer event m where

    index timedEvent ix = let

        appendEvent :: ListIndexer event -> ListIndexer event
        appendEvent = events %~ (timedEvent:)

        updateLatest :: ListIndexer event -> ListIndexer event
        updateLatest = latest ?~ (timedEvent ^. point)

        checkOverflow :: Bool -> m ()
        checkOverflow b = when b $ throwError $ NoSpaceLeft ix

        in do
            checkOverflow =<< isFull ix
            pure $ ix
                & appendEvent
                & updateLatest

instance Applicative m => IsSync ListIndexer event m where
    lastSyncPoint = pure . view latest

instance Applicative m => Rewindable ListIndexer event m where

    rewind p ix = let

        adjustLatestPoint :: ListIndexer event -> ListIndexer event
        adjustLatestPoint = latest ?~ p
        cleanEventsAfterRollback :: ListIndexer event -> ListIndexer event
        cleanEventsAfterRollback = events %~ dropWhile isEventAfterRollback
        isIndexBeforeRollback :: ListIndexer event -> Bool
        isIndexBeforeRollback x = maybe True (p >=) $ x ^. latest
        isEventAfterRollback :: TimedEvent event -> Bool
        isEventAfterRollback = (p <) . view point

        in pure . pure
        $ if isIndexBeforeRollback ix
             then ix -- if we're already before the rollback, we don't have to do anything
             else ix
                & cleanEventsAfterRollback
                & adjustLatestPoint

instance Applicative m => Resumable ListIndexer event m where

    syncPoints ix = let

      indexPoints = ix ^.. events . folded . point
      -- if the latest point of the index is not a stored event, we add it to the list of points
      addLatestIfNeeded Nothing ps         = ps
      addLatestIfNeeded (Just p) []        = [p]
      addLatestIfNeeded (Just p) ps@(p':_) = if p == p' then ps else p:ps

      in pure $ addLatestIfNeeded (ix ^. latest) indexPoints

-- ** Mixed indexer

-- | An indexer that has at most '_blocksListIndexer' events in memory and put the older one in database.
-- The query interface for this indexer will alwyas go through the database first and then aggregate
-- results present in memory.
--
-- @mem@ the indexer that handle old events, when we need to remove stuff from memory
-- @store@ the indexer that handle the most recent events
data MixedIndexer mem store event = MixedIndexer
    { _inMemory   :: !(mem event) -- ^ The fast storage for latest elements
    , _inDatabase :: !(store event) -- ^ In database storage, should be similar to the original indexer
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
    , BoundedMemory mem m
    , Foldable (Container mem)
    , Eq (Point event)
    ) => MixedIndexer mem store event ->
    m (MixedIndexer mem store event)
flush indexer = do
    (eventsToFlush, indexer') <- getCompose $ inMemory (Compose . flushMemory) indexer
    inDatabase (indexAll eventsToFlush) indexer'

instance
    ( Monad m
    , BoundedMemory mem m
    , Foldable (Container mem)
    , IsIndex mem event m
    , IsIndex store event m
    ) => IsIndex (MixedIndexer mem store) event m where

    index timedEvent indexer = do
        full <- isFull $ indexer ^. inMemory
        if full
           then do
               indexer' <- flush indexer
               inMemory (index timedEvent) indexer'
           else inMemory (index timedEvent) indexer

instance IsSync mem event m => IsSync (MixedIndexer mem store) event m where
    lastSyncPoint = lastSyncPoint . view inMemory

instance
    ( Monad m
    , Rewindable store event m
    ) => Rewindable (MixedIndexer ListIndexer store) event m where

    rewind p indexer = let

        rewindInStore :: Rewindable index event m => index event -> MaybeT m (index event)
        rewindInStore = MaybeT . rewind p

        in runMaybeT $ do
            ix <- inMemory rewindInStore indexer
            guard $ not $ null $ ix ^. inMemory . events
            inDatabase rewindInStore ix

instance
    ( ResumableResult ListIndexer event query m
    , Queryable store event query m
    ) =>
    Queryable (MixedIndexer ListIndexer store) event query m where

    query valid q indexer
        = resumeResult valid q
            (indexer ^. inMemory)
            (query valid q (indexer ^. inDatabase))


-- ** Runners

-- | The different types of input of a runner
data ProcessedInput event
   = Rollback !(Point event)
   | Index !(TimedEvent event)

-- * Runners

-- | A runner encapsulate an indexer in an opaque type.
-- It allows us to manipulate seamlessly a list of indexers that has different types
-- as long as they implement the required interfaces.
data RunnerM m input point =
    forall indexer event.
    ( IsIndex indexer event m
      , IsSync indexer event IO
    , Resumable indexer event m
    , Rewindable indexer event m
    , Point event ~ point
    ) =>
    Runner
        { runnerState  :: !(TMVar (indexer event))

        , processInput :: !(input -> m (ProcessedInput event))
          -- ^ used by the runner to check whether an input is a rollback or an event
        }

type Runner = RunnerM IO

-- | create a runner for an indexer, retuning the runner and the 'MVar' it's using internally
createRunner ::
    ( IsIndex indexer event IO
    , IsSync indexer event IO
    , Resumable indexer event IO
    , Rewindable indexer event IO
    , point ~ Point event) =>
    indexer event ->
    (input -> IO (ProcessedInput event)) ->
    IO (TMVar (indexer event), Runner input point)
createRunner ix f = do
  mvar <- STM.atomically $ STM.newTMVar ix
  pure (mvar, Runner mvar f)

-- | The runner notify its coordinator that it's ready
-- and starts waiting for new events and process them as they come
startRunner :: Ord point => TChan input -> QSemN -> Runner input point -> IO ()
startRunner chan tokens (Runner ix processInput) = let

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

    handleRollback p = do
        indexer <- STM.atomically $ STM.takeTMVar ix
        mindexer <- rewind p indexer
        maybe
            (STM.atomically $ STM.putTMVar ix indexer)
            (STM.atomically . STM.putTMVar ix)
            mindexer

    in do
        chan' <- STM.atomically $ STM.dupTChan chan
        input <- STM.atomically $ STM.readTChan chan'
        forever $ do
            unlockCoordinator
            processedInput <- processInput input
            case processedInput of
                 Rollback p -> handleRollback p
                 Index e    -> indexEvent e

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
runnerSyncPoints (r:rs) = let

    getSyncPoints :: Ord point => Runner input point -> IO [point]
    getSyncPoints (Runner ix _) = do
        indexer <- STM.atomically $ STM.readTMVar ix
        syncPoints indexer

    in do
        ps <- getSyncPoints r
        foldlM (\acc r' -> intersect acc <$> getSyncPoints r') ps rs

-- | create a coordinator with started runners
start :: Ord point => [Runner input point] -> IO (Coordinator input point)
start runners' = let

    startRunners channel' tokens' = traverse_ (startRunner channel' tokens') runners'

    in do
        let nb = length runners'
        tokens' <- Con.newQSemN 0 -- starts empty, will be filled when the runners will start
        channel' <- STM.newBroadcastTChanIO
        startRunners channel' tokens'
        pure $ Coordinator Nothing runners' tokens' channel' nb

-- A coordinator step (send an input to its runners, wait for an ack of every runner before listening again)
step :: (input -> point) -> Coordinator input point -> input -> IO (Coordinator input point)
step getPoint coordinator input = let

      waitRunners = Con.waitQSemN (coordinator ^. tokens) (coordinator ^. nbRunners)

      dispatchNewInput = STM.atomically $ STM.writeTChan (coordinator ^. channel) input

      setLastSync c = c & lastSync ?~ getPoint input

    in do
        dispatchNewInput
        waitRunners $> setLastSync coordinator

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

instance IsSync CoordinatorIndex event IO where
    lastSyncPoint indexer = pure $ indexer ^. coordinator . lastSync

-- | To rewind a coordinator, we try and rewind all the runners.
instance Rewindable CoordinatorIndex event IO where

    rewind p = let

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

        in  runMaybeT . coordinator rewindRunners

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
    Queryable ListIndexer event (EventAtQuery event) m where

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
    ResumableResult ListIndexer event (EventAtQuery event) m where

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
    Queryable ListIndexer event (EventsMatchingQuery event) m where

    query p q ix = do
        let isBefore e p' = e ^. point <= p'
        let result = EventsMatching $ ix ^.. events
                         . folded . filtered (`isBefore` p)
                         . event . filtered (predicate q)
        check <- isNotAheadOfSync p ix
        if check
            then pure result
            else throwError . AheadOfLastSync . Just $ result

instance MonadError (QueryError (EventsMatchingQuery event)) m =>
    ResumableResult ListIndexer event (EventsMatchingQuery event) m where

    resumeResult p q indexer result = result `catchError` \case
         -- If we find an incomplete result in the first indexer, complete it
        AheadOfLastSync (Just r) -> (<> r) <$> query p q indexer
        inDatabaseError          -> throwError inDatabaseError -- For any other error, forward it


-- * Indexer transformer: modify the behaviour of a indexer

-- ** Tracer Add tracing to an existing indexer

-- | A tracer modifier that adds tracing to an existing indexer
data WithTracer m indexer event
    = WithTracer
    { _tracedIndexer :: !(indexer event)
    , _tracer        :: !(Tracer m (ProcessedInput event))
    }

makeLenses 'WithTracer

instance
    (Monad m, IsIndex index event m) =>
    IsIndex (WithTracer m index) event m where

    index timedEvent indexer = do
        res <- tracedIndexer (index timedEvent) indexer
        Tracer.traceWith (indexer ^. tracer) $ Index timedEvent
        pure res

instance IsSync index event m => IsSync (WithTracer m index) event m where
    lastSyncPoint = lastSyncPoint . view tracedIndexer

instance
    ( Monad m
    , Rewindable index event m
    ) => Rewindable (WithTracer m index) event m where

    rewind p indexer = let

         rewindWrappedIndexer p' = tracedIndexer (MaybeT . rewind p') indexer

         traceSuccessfulRewind indexer' = do
              Tracer.traceWith (indexer' ^. tracer) (Rollback p)
              pure indexer'

        in do
        res <- runMaybeT $ rewindWrappedIndexer p
        traverse traceSuccessfulRewind res

instance Queryable indexer event query m =>
    Queryable (WithTracer m indexer) event query m where

    query p q indexer = query p q (indexer ^. tracedIndexer)

-- ** Delaying insert

-- | When indexing computation is expensive, you may want to delay it to avoid expensive rollback
-- 'WithDelay' buffers events before sending them to the underlying indexer.
-- Buffered events are sent when the buffers overflows.
--
-- An indexer wrapped in 'WithDelay' won't interact nicely with coordinator at the moment,
-- as 'WithDelay' acts as it's processing an event while it only postpones the processing.
data WithDelay index event
    = WithDelay
    { _delayedIndexer :: !(index event)
    , _bufferCapacity :: !Word
    , _bufferSize     :: !Word
    , _buffer         :: !(Seq (TimedEvent event))
    }

makeLenses 'WithDelay

instance
    (Monad m, IsIndex indexer event m) =>
    IsIndex (WithDelay indexer) event m where

    index timedEvent indexer = let

        bufferIsFull b = (b ^. bufferSize) >= (b ^. bufferCapacity)

        bufferEvent = (bufferSize +~ 1) . (buffer %~ (timedEvent <|))

        pushAndGetOldest = \case
            Empty            -> (timedEvent, Empty)
            (buffer' :|> e') -> (e', timedEvent <| buffer')

        in do
        if not $ bufferIsFull indexer
        then pure $ bufferEvent indexer
        else do
            let b = indexer ^. buffer
                (oldest, buffer') = pushAndGetOldest b
            res <- delayedIndexer (index oldest) indexer
            pure $ res & buffer .~ buffer'

instance IsSync index event m => IsSync (WithDelay index) event m where

    lastSyncPoint = lastSyncPoint . view delayedIndexer

instance
    ( Monad m
    , Rewindable indexer event m
    , Ord (Point event)
    ) => Rewindable (WithDelay indexer) event m where

    rewind p indexer = let

        rewindWrappedIndexer p' = delayedIndexer (MaybeT . rewind p') indexer

        resetBuffer = (bufferSize .~ 0) . (buffer .~ Seq.empty)

        (after, before) =  Seq.spanl ((> p) . view point) $ indexer ^. buffer

        in if Seq.null before
           then runMaybeT $ resetBuffer <$> rewindWrappedIndexer p
           else pure . pure $ indexer
                   & buffer .~ after
                   & bufferSize .~ fromIntegral (Seq.length after)

instance Queryable indexer event query m =>
    Queryable (WithDelay indexer) event query m where

    query p q indexer = query p q (indexer ^. delayedIndexer)

-- ** Aggregation control


-- | WithAggregate control when we should aggregate an indexer
data WithAggregate indexer event
    = WithAggregate
    { _aggregatedIndexer :: !(indexer event)
      -- ^ the underlying indexer
    , _securityParam     :: !Word
      -- ^ how far can a rollback go
    , _aggregateEvery    :: !Word
      -- ^ how once we have enough events, how often do we rollback
    , _nextAggregates    :: !(Seq (Point event))
      -- ^ list of aggregation points
    , _stepsBeforeNext   :: !Word
      -- ^ events requires before next aggregation milestones
    , _currentDepth      :: !Word
      -- ^ how many events aren't aggregated yet
    }

makeLenses ''WithAggregate

aggregateAt
    :: WithAggregate indexer event
    -> Maybe (Point event, WithAggregate indexer event)
aggregateAt indexer = let

    reachAggregationPoint = indexer ^. currentDepth >= indexer ^. securityParam + indexer ^. aggregateEvery

    dequeueNextAggregatePoint =
        case indexer ^. nextAggregates of
            Empty    -> Nothing
            xs :|> p -> Just (p, indexer & nextAggregates .~ xs)

    in guard reachAggregationPoint *> dequeueNextAggregatePoint


startNewStep :: Point event -> WithAggregate indexer event -> WithAggregate indexer event
startNewStep p indexer
    = indexer
        & nextAggregates %~ (p <|)
        & stepsBeforeNext .~ (indexer ^. aggregateEvery)

tick
    :: Point event
    -> WithAggregate indexer event
    -> (Maybe (Point event), WithAggregate indexer event)
tick p indexer = let

    countEvent = (currentDepth +~ 1) . (stepsBeforeNext -~ 1)

    adjustStep ix = if ix ^. stepsBeforeNext == 0
        then startNewStep p ix
        else ix

    indexer' = adjustStep $ countEvent indexer

    in maybe (Nothing, indexer') (first Just) $ aggregateAt indexer'


instance
    (Monad m, Aggregable indexer event m, IsIndex indexer event m) =>
    IsIndex (WithAggregate indexer) event m where

    index timedEvent indexer = do
        indexer' <- aggregatedIndexer (index timedEvent) indexer
        let (mp, indexer'') = tick (timedEvent ^. point) indexer'
        maybe
          (pure indexer'')
          (\p -> aggregatedIndexer (aggregate p) indexer)
          mp

instance IsSync index event m => IsSync (WithAggregate index) event m where

    lastSyncPoint = lastSyncPoint . view aggregatedIndexer

instance Queryable indexer event query m =>
    Queryable (WithAggregate indexer) event query m where

    query p q indexer = query p q (indexer ^. aggregatedIndexer)

-- | The rewindable instance for `WithAggregate` is a defensive heuristic
-- that may provide a non optimal behaviour but ensure that we don't
-- mess up with the rollbackable events.
instance
    ( Monad m
    , Rewindable indexer event m
    , Ord (Point event)
    ) => Rewindable (WithAggregate indexer) event m where

    rewind p indexer = let

        rewindWrappedIndexer p' = aggregatedIndexer (MaybeT . rewind p')
        resetStep = do
            stepLength <- view aggregateEvery
            set stepsBeforeNext stepLength

        cleanAggregatePoints p' = nextAggregates %~ Seq.dropWhileL (> p')
        countFromAggregatePoints = do
            points <- view nextAggregates
            stepLength <- view aggregateEvery
            -- We can safely consider that for each aggregate point still in the pipe,
            -- we have 'stepLength' events available in the indexer
            set currentDepth (fromIntegral $ length points * fromIntegral stepLength)

        in runMaybeT $
            countFromAggregatePoints
            . cleanAggregatePoints p
            . resetStep
            <$> rewindWrappedIndexer p indexer

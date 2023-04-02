{-# LANGUAGE DerivingVia          #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{- |
 This module propose an alternative to the index implementation proposed in 'RewindableIndex.Storable'.

 The point we wanted to address are the folowing:

    * 'Storable' implementation is designed in a way that strongly promotes indexers
      that rely on a mix of database and in-memory storage.
      We try to propose a more generic design that would allow:

        * full in-memory indexers
        * indexer backed by a simple file
        * indexer transformers, that add capability (logging, caching...) to an indexer
        * mock indexer, for testing purpose, with predefined behaviour
        * group of indexers, synchronised as a single indexer
        * implement in-memory/database storage that rely on other query heuristic

    * The original implementation considered the 'StorablePoint' as data that can be derived from 'Event',
      leading to the design of synthetic events to deal with indexer that didn't index enough data.

    * In marconi, the original design uses a callback design to handle `MVar` modification,
      we wanted to address this point as well.

What's included in this module:

    * Base type classes to define an indexer, its query interface, and the required plumbing to handle rollback.
    * A full in-memory indexer (naive), a full SQLite indexer
      and an indexer that compose it with a SQL layer for persistence.
    * A coordinator for indexers, that can be exposed as an itdexer itself.
    * Some queries that can be applied to many indexers.
    * Several modifiers for indexers:
        * Tracing, as a modifier to an existing indexer.
          (it allows us to opt-in for traces if we want, indexer by indexer)
        * Delay to delay event processing for heavy computation.
        * Pruning, to compact data that can't be rollbacked.

  Contrary to the original Marconi design, indexers don't have a unique (in-memory/sqlite) implementation.

  (non-exhaustive) TODO list:

    * Provide more typeclasses implementation for an SQLite indexer.
      We shouldn't have to provide more than the queries and tables in most cases.
      The indexer instances should take care of the global behaviour for all typeclasses.
    * Provide a less naive in-memory indexer implementation than the list one
    * Test, test, test. The current code is not tested, and it's wrong.
      Ideally, we should be able to provide a model-based testing approach to specify
      what we expect from indexers.
    * Re-implement some of our indexers.
    * Split up this mess in modules.
    * Generate haddock, double-check it, fill the void.
    * Provide a tutorial on how to write indexer, transformers, and how to instantiate them.
    * Cold start from disk.
    * Provide MonadState version of the functions that manipulates the indexer.

-}
module Marconi.Core.Experiment
    (
    -- * Core types and typeclasses
    -- ** Core types
      Point
    , Result (..)
    , Container
    , TimedEvent
    -- ** Core typeclasses
    , IsIndex (..)
    , IsSync (..)
    , isNotAheadOfSync
    , Queryable (..)
    , query'
    , ResumableResult (..)
    , Rewindable (..)
    , Prunable (..)
    , Resumable (..)
    -- ** Errors
    , IndexError (..)
    , QueryError (..)
    -- * Core Indexers
    -- ** In memory
    , ListIndexer
        , capacity
        , events
        , latest
    -- ** In database
    , SQLiteIndexer
        , handle
        , prepareInsert
        , buildInsert
        , dbLastSync
    , MixedIndexer
        , mixedIndexer
        , inMemory
        , inDatabase
    , IndexQuery (..)
    , InsertRecord
    , singleInsertSQLiteIndexer
    -- Running indexers
    -- ** Runners
    , RunnerM (..)
    , Runner
    , createRunner
    , startRunner
    , ProcessedInput (..)
    -- ** Coordinator
    , Coordinator
        , lastSync
        , runners
        , tokens
        , channel
        , nbRunners
    , start
    , step
    , CoordinatorIndex
        , coordinator
    -- * Common queries
    --
    -- Queries that can be implemented for all indexers
    , EventAtQuery (..)
    , EventsMatchingQuery (..)
    -- * Indexer Transformers
    -- ** Tracer
    , WithTracer
        , withTracer
        , tracedIndexer
        , tracer
    -- ** Delay
    , WithDelay
        , withDelay
        , delayedIndexer
        , delayCapacity
        , delayLength
        , delayBuffer
    -- ** Control Pruning
    , WithPruning
        , withPruning
        , prunedIndexer
        , securityParam
        , pruneEvery
        , nextPruning
        , stepsBeforeNext
        , currentDepth
    -- ** Index wrapper
    , IndexWrapper
        , wrappedIndexer
        , wrapperConfig
    , indexVia
    , lastSyncPointVia
    , queryVia
    , syncPointsVia
    , pruneVia
    , pruningPointVia
    , rewindVia
    ) where

import Control.Concurrent qualified as Con (newQSemN, signalQSemN, waitQSemN)
import Control.Concurrent.STM qualified as STM (atomically, dupTChan, newBroadcastTChanIO, newTMVar, putTMVar,
                                                readTChan, readTMVar, takeTMVar, writeTChan)
import Control.Tracer qualified as Tracer (traceWith)
import Data.Sequence qualified as Seq

import Control.Concurrent (QSemN)
import Control.Lens (Getter, Lens', filtered, folded, makeLenses, set, view, (%~), (&), (+~), (-~), (.~), (<<.~), (?~),
                     (^.), (^..), (^?))
import Control.Monad (forever, guard)
import Control.Monad.Except (ExceptT, MonadError (catchError, throwError), runExceptT)
import Control.Tracer (Tracer)

import Control.Concurrent.Async (mapConcurrently_)
import Control.Concurrent.STM (TChan, TMVar)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Bifunctor (first)
import Data.Foldable (foldlM, foldrM, traverse_)
import Data.Functor (($>))
import Data.Functor.Compose (Compose (Compose, getCompose))
import Data.List (intersect)
import Data.Maybe (listToMaybe)
import Data.Sequence (Seq (Empty, (:|>)), (<|))
import Data.Text (Text)
import Database.SQLite.Simple qualified as SQL


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
-- The indexer should provide two main functionalities:
-- indexing events, and providing its last synchronisation point.
--
--     * @indexer@ the indexer implementation type
--     * @event@ the indexed events
--     * @m@ the monad in which our indexer operates
class Monad m => IsIndex m event indexer where

    -- | index an event at a given point in time
    index :: Eq (Point event) =>
        TimedEvent event -> indexer event -> m (indexer event)

    -- | Index a bunch of points, associated to their event, in an indexer
    indexAll :: (Eq (Point event), Traversable f) =>
        f (TimedEvent event) -> indexer event -> m (indexer event)
    indexAll = flip (foldrM index)

    {-# MINIMAL index #-}

class IsSync m event indexer where

    -- | Last sync of the indexer
    lastSyncPoint :: indexer event -> m (Maybe (Point event))

-- | Check if the given point is ahead of the last syncPoint of an indexer,
isNotAheadOfSync ::
    (Ord (Point event), IsSync m event indexer, Functor m) =>
    Point event -> indexer event -> m Bool
isNotAheadOfSync p indexer = maybe False (> p) <$> lastSyncPoint indexer


-- | Error that can occurs when you query an indexer
data QueryError query
   = AheadOfLastSync !(Maybe (Result query))
     -- ^ The required point is ahead of the current index.
     -- The error may still provide its latest result if it make sense for the given query.
     --
     -- It can be useful for indexer that contains a partial knowledge and that want to pass
     -- this knowledge to another indexer to complete the query.
   | NotStoredAnymore
     -- ^ The requested point is too far in the past and has been pruned
   | IndexerQueryError !Text
     -- ^ The indexer query failed

-- | The indexer can answer a Query to produce the corresponding result of that query.
--
--     * @indexer@ is the indexer implementation type
--     * @event@ the indexer events
--     * @query@ the type of query we want to answer
--     * @m@ the monad in which our indexer operates
class Queryable m event query indexer where

    -- | Query an indexer at a given point in time
    -- It can be read as:
    -- "With the knowledge you have at that point in time,
    --  what is your answer to this query?"
    query :: Ord (Point event) => Point event -> query -> indexer event -> m (Result query)

-- | Like @query@, but internalise @QueryError@ in the result.
query'
    :: (Queryable (ExceptT (QueryError query) m) event query indexer, Ord (Point event))
    => Point event -> query -> indexer event -> m (Either (QueryError query) (Result query))
query' p q = runExceptT . query p q

-- | The indexer can take a result and complete it with its events
class ResumableResult m event query indexer where

    resumeResult ::
       Ord (Point event) =>
       Point event -> query -> indexer event -> m (Result query) -> m (Result query)

-- | We can reset an indexer to a previous `Point`
--     * @indexer@ is the indexer implementation type
--     * @event@ the indexer events
--     * @m@ the monad in which our indexer operates
class Rewindable m event indexer where

    rewind :: Ord (Point event) => Point event -> indexer event -> m (Maybe (indexer event))

-- | The indexer can prune old data.
-- The main purpose is to speed up query processing.
-- If the indexer is 'Rewindable' and 'Prunable',
-- it can't 'rewind' behind the 'pruningPoint',
-- the idea is to call 'prune' on points that can't be rollbacked anymore.
--
--     * @indexer@ is the indexer implementation type
--     * @desc@ the descriptor of the indexer, fixing the @Point@ types
--     * @m@ the monad in which our indexer operates
class Prunable m event indexer where

    -- Prune events of the indexer up to a given point in time
    prune :: Ord (Point event) => Point event -> indexer event -> m (indexer event)

    -- The latest pruned point (events up to the result are pruned)
    pruningPoint :: indexer event -> m (Maybe (Point event))

-- | Points from which we can restract safely
class Resumable m event indexer where

    -- | List the points that we still have in the indexers, allowing us to restart from them
    syncPoints :: Ord (Point event) => indexer event -> m [Point event]


-- * Base indexers

-- ** Full in-memory indexer

-- | How events can be extracted from an indexer
type family Container (indexer :: * -> *) :: * -> *

-- | Define an in-memory container with a limited memory
class Flushable m indexer where

    -- | Check if there isn't space left in memory
    isFull :: indexer event -> m Bool

    -- | Clear the memory and return its content
    flushMemory
        :: Word
           -- ^ How many event do we keep
        -> indexer event
        -> m (Container indexer (TimedEvent event), indexer event)

-- | A Full in memory indexer, it uses list because I was too lazy to port the 'Vector' implementation.
-- If we wanna move to these indexers, we should switch the implementation to the 'Vector' one.
data ListIndexer event =
    ListIndexer
    { _capacity :: !Word
    , _events   :: ![TimedEvent event] -- ^ Stored 'Event', associated with their history 'Point'
    , _latest   :: !(Maybe (Point event)) -- ^ Ease access to the latest sync point
    }

type instance Container ListIndexer = []

makeLenses 'ListIndexer

instance Applicative m => Flushable m ListIndexer where

    isFull ix = pure $ ix ^. capacity >= fromIntegral (length (ix ^. events))

    flushMemory _ ix = pure $ ix & events <<.~ []

instance
    (MonadError (IndexError ListIndexer event) m, Monad m) =>
    IsIndex m event ListIndexer where

    index timedEvent ix = let

        appendEvent :: ListIndexer event -> ListIndexer event
        appendEvent = events %~ (timedEvent:)

        updateLatest :: ListIndexer event -> ListIndexer event
        updateLatest = latest ?~ (timedEvent ^. point)

        in do
            pure $ ix
                & appendEvent
                & updateLatest

instance Applicative m => IsSync m event ListIndexer where
    lastSyncPoint = pure . view latest

instance Applicative m => Rewindable m event ListIndexer where

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

instance Applicative m => Resumable m event ListIndexer where

    syncPoints ix = let

      indexPoints = ix ^.. events . folded . point
      -- if the latest point of the index is not a stored event, we add it to the list of points
      addLatestIfNeeded Nothing ps         = ps
      addLatestIfNeeded (Just p) []        = [p]
      addLatestIfNeeded (Just p) ps@(p':_) = if p == p' then ps else p:ps

      in pure $ addLatestIfNeeded (ix ^. latest) indexPoints


-- | When we want to store an event in a database, it may happen that you want to store it in many tables,
-- ending with several insert.
--
-- This leads to two major issues:
--     - Each query has its own parameter type, we consequently don't have a unique type for a parametrised query.
--     - When we perform the insert, we want to process in the same way all the queries.
--     - We can't know in the general case neither how many query will be needed, nor the param types.
--     - We want to minimise the boilerplate for a end user.
--
-- To tackle these issue, we wrap our queries in a opaque type, @IndexQuery@,
-- which hides the query parameters.
-- Internally, we only have to deal with a @[IndexQuery]@ to be able to insert an event.
data IndexQuery
    = forall param.
    SQL.ToRow param =>
    IndexQuery
        { insertQuery :: !SQL.Query
        , params      :: ![param]
         -- ^ It's a list because me want to be able to deal with bulk insert,
         -- which is often required for performance reasons in Marconi.
        }

-- | Run a list of insert queries in one single transaction.
runIndexQueries :: SQL.Connection -> [IndexQuery] -> IO ()
runIndexQueries _ [] = pure ()
runIndexQueries c xs = let
    runIndexQuery (IndexQuery insertQuery params) = SQL.executeMany c insertQuery params
    in SQL.withTransaction c
        $ mapConcurrently_ runIndexQuery xs

-- | How we map an event to its sql representation
--
-- In general, it consists in breaking the event in many fields of a record,
-- each field correspondind to the parameters required to insert a part of the event in one table.
type family InsertRecord event

-- | Provide the minimal elements required to use a SQLite database to back an indexer.
data SQLiteIndexer event
    = SQLiteIndexer
        { _handle        :: !SQL.Connection
          -- ^ The connection used to interact with the database
        , _prepareInsert :: !(TimedEvent event -> InsertRecord event)
          -- ^ 'insertRecord' is the typed representation of what has to be inserted in the database
          -- It should be a monoid, to allow insertion of 0 to n rows in a single transaction
        , _buildInsert   :: !(InsertRecord event -> [IndexQuery])
          -- ^ Map the 'insertRecord' representation to 'IndexQuery',
          -- to actually performed the insertion in the database.
          -- One can think at the insert record as a typed representation of the parameters of the queries,
          -- that can be bundle with the query in the opaque @IndexQuery@ type.
        , _dbLastSync    :: !SQL.Query
          -- ^ The query to extract the latest sync point from the database.
        }

makeLenses ''SQLiteIndexer

-- | A smart constructor for indexer that want to map an event to a single table.
-- We just have to set the type family of `InsertRecord event` to `[param]` and
-- then to provide the expected parameters.
singleInsertSQLiteIndexer
    :: SQL.ToRow param
    => InsertRecord event ~ [param]
    => SQL.Connection
    -> (TimedEvent event -> param)
    -- ^ extract 'param' out of a 'TimedEvent'
    -> SQL.Query
    -- ^ the insert query
    -> SQL.Query
    -- ^ the last sync query
    -> SQLiteIndexer event
singleInsertSQLiteIndexer c toParam insertQuery lastSyncQuery
    = SQLiteIndexer
        {_handle = c
        , _prepareInsert = pure . toParam
        , _buildInsert = pure . IndexQuery insertQuery
        , _dbLastSync = lastSyncQuery
        }

instance (MonadIO m, Monoid (InsertRecord event)) =>
    IsIndex m event SQLiteIndexer where

    index evt indexer = liftIO $ do
        let indexQueries = indexer ^. buildInsert $ indexer ^. prepareInsert $ evt
        runIndexQueries (indexer ^. handle) indexQueries
        pure indexer

    indexAll evts indexer = liftIO $ do
        let indexQueries = indexer ^. buildInsert $ foldMap (indexer ^. prepareInsert) evts
        runIndexQueries (indexer ^. handle) indexQueries
        pure indexer

instance (SQL.FromRow (Point event), MonadIO m) =>
    IsSync m event SQLiteIndexer where

    lastSyncPoint indexer
        = liftIO $ listToMaybe
        <$> SQL.query_ (indexer ^. handle) (indexer ^. dbLastSync)



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
    ( IsIndex m event indexer
    , IsSync m event indexer
    , Resumable m event indexer
    , Rewindable m event indexer
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
    ( IsIndex IO event indexer
    , IsSync IO event indexer
    , Resumable IO event indexer
    , Rewindable IO event indexer
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

    unlockCoordinator :: IO ()
    unlockCoordinator = Con.signalQSemN tokens 1

    fresherThan :: Ord (Point event) => TimedEvent event -> Maybe (Point event) -> Bool
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


-- TODO handwrite lenses to avoid invalid states
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

      waitRunners :: IO ()
      waitRunners = Con.waitQSemN (coordinator ^. tokens) (coordinator ^. nbRunners)

      dispatchNewInput :: IO ()
      dispatchNewInput = STM.atomically $ STM.writeTChan (coordinator ^. channel) input

      setLastSync c = c & lastSync ?~ getPoint input

    in do
        dispatchNewInput
        waitRunners $> setLastSync coordinator

-- A 'Coordinator', viewed as an indexer
newtype CoordinatorIndex event =
     CoordinatorIndex
          { _coordinator :: Coordinator event (Point event)
          }

makeLenses 'CoordinatorIndex

-- A coordinator can be consider as an indexer that forwards the input to its runner
instance IsIndex IO event CoordinatorIndex where

    index timedEvent = coordinator $
            \x -> step (const $ timedEvent ^. point) x $ timedEvent ^. event

instance IsSync IO event CoordinatorIndex where
    lastSyncPoint indexer = pure $ indexer ^. coordinator . lastSync

-- | To rewind a coordinator, we try and rewind all the runners.
instance Rewindable IO event CoordinatorIndex where

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


-- | Get the event stored by the indexer at a given point in time
data EventAtQuery event = EventAtQuery

-- | The result of EventAtQuery is always an event.
-- The error cases are handled by the query interface.
-- in time
newtype instance Result (EventAtQuery event) =
    EventAtResult {getEvent :: event}

instance MonadError (QueryError (EventAtQuery event)) m =>
    Queryable m event (EventAtQuery event) ListIndexer where

    query p EventAtQuery ix = do
        let isAtPoint e p' = e ^. point == p'
        check <- isNotAheadOfSync p ix
        if check
        then maybe
             -- If we can't find the point and if it's in the past, we probably pruned it
            (throwError NotStoredAnymore)
            (pure . EventAtResult)
            $ ix ^? events . folded . filtered (`isAtPoint` p) . event
        else throwError $ AheadOfLastSync Nothing

instance MonadError (QueryError (EventAtQuery event)) m =>
    ResumableResult m event (EventAtQuery event) ListIndexer where

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
    Queryable m event (EventsMatchingQuery event) ListIndexer where

    query p q ix = do
        let isAfter p' e = p' > e ^. point
        let result = EventsMatching $ ix ^.. events
                         . folded . filtered (isAfter p)
                         . event . filtered (predicate q)
        check <- isNotAheadOfSync p ix
        if check
            then pure result
            else throwError . AheadOfLastSync . Just $ result

instance MonadError (QueryError (EventsMatchingQuery event)) m =>
    ResumableResult m event (EventsMatchingQuery event) ListIndexer where

    resumeResult p q indexer result = result `catchError` \case
         -- If we find an incomplete result in the first indexer, complete it
        AheadOfLastSync (Just r) -> (<> r) <$> query p q indexer
        inDatabaseError          -> throwError inDatabaseError -- For any other error, forward it



-- | Wrap an indexer with some extra information to modify its behaviour
--
--
-- The wrapeer comes with some instances that relay the function to the wrapped indexer,
-- without any extra behaviour.
--
-- A real wrapper can be a new type of 'IndexWrapper', reuse some of its instances with @deriving via@,
-- and specify its own instances when it wants to add logic in it.
data IndexWrapper config indexer event
    = IndexWrapper
        { _wrapperConfig  :: !(config event)
        , _wrappedIndexer :: !(indexer event)
        }

makeLenses 'IndexWrapper

-- | Helper to implement the @index@ functon of 'IsIndex' when we use a wrapper.
-- If you don't want to perform any other side logic, use @deriving via@ instead.
indexVia
    :: (IsIndex m event indexer, Eq (Point event))
    => Lens' s (indexer event) -> TimedEvent event -> s -> m s
indexVia l = l . index

instance
    (Monad m, IsIndex m event indexer) =>
    IsIndex m event (IndexWrapper config indexer) where

    index = indexVia wrappedIndexer

-- | Helper to implement the @lastSyncPoint@ functon of 'IsSync' when we use a wrapper.
-- If you don't want to perform any other side logic, use @deriving via@ instead.
lastSyncPointVia
    :: IsSync m event indexer
    => Getter s (indexer event) -> s -> m (Maybe (Point event))
lastSyncPointVia l = lastSyncPoint . view l

instance IsSync event m index =>
    IsSync event m (IndexWrapper config index) where

    lastSyncPoint = lastSyncPointVia wrappedIndexer

-- | Helper to implement the @query@ functon of 'Queryable' when we use a wrapper.
-- If you don't want to perform any other side logic, use @deriving via@ instead.
queryVia
    :: (Queryable m event query indexer, Ord (Point event))
    => Getter s (indexer event)
    -> Point event -> query -> s -> m (Result query)
queryVia l p q = query p q . view l

instance Queryable m event query indexer =>
    Queryable m event query (IndexWrapper config indexer) where

    query =  queryVia wrappedIndexer

-- | Helper to implement the @query@ functon of 'Resumable' when we use a wrapper.
-- If you don't want to perform any other side logic, use @deriving via@ instead.
syncPointsVia
    :: (Resumable m event indexer, Ord (Point event))
    => Getter s (indexer event) -> s -> m [Point event]
syncPointsVia l = syncPoints . view l


instance Resumable m event indexer =>
    Resumable m event (IndexWrapper config indexer) where

    syncPoints = syncPointsVia wrappedIndexer

-- | Helper to implement the @prune@ functon of 'Prunable' when we use a wrapper.
-- Unfortunately, as 'm' must have a functor instance, we can't use @deriving via@ directly.
pruneVia
    :: (Functor m, Prunable m event indexer, Ord (Point event))
    => Lens' s (indexer event) -> Point event -> s -> m s
pruneVia l = l . prune

-- | Helper to implement the @pruningPoint@ functon of 'Prunable' when we use a wrapper.
-- Unfortunately, as 'm' must have a functor instance, we can't use @deriving via@ directly.
pruningPointVia
    :: Prunable m event indexer
    => Getter s (indexer event) -> s -> m (Maybe (Point event))
pruningPointVia l = pruningPoint . view l

-- | Helper to implement the @rewind@ functon of 'Rewindable' when we use a wrapper.
-- Unfortunately, as 'm' must have a functor instance, we can't use @deriving via@ directly.
rewindVia
    :: (Functor m, Rewindable m event indexer, Ord (Point event))
    => Lens' s (indexer event)
    -> Point event -> s -> m (Maybe s)
rewindVia l p = runMaybeT . l (MaybeT . rewind p)


newtype ProcessedInputTracer m event = ProcessedInputTracer { _unwrapTracer :: Tracer m (ProcessedInput event)}

makeLenses 'ProcessedInputTracer

-- | A tracer modifier that adds tracing to an existing indexer
newtype WithTracer m indexer event
    = WithTracer { _tracerWrapper :: IndexWrapper (ProcessedInputTracer m) indexer event }

withTracer :: Tracer m (ProcessedInput event) -> indexer event -> WithTracer m indexer event
withTracer tr = WithTracer . IndexWrapper (ProcessedInputTracer tr)

makeLenses 'WithTracer

deriving via (IndexWrapper (ProcessedInputTracer m) indexer)
    instance IsSync m event indexer => IsSync m event (WithTracer m indexer)

deriving via (IndexWrapper (ProcessedInputTracer m) indexer)
    instance Queryable m event query indexer => Queryable m event query (WithTracer m indexer)

deriving via (IndexWrapper (ProcessedInputTracer m) indexer)
    instance Resumable m event indexer => Resumable m event (WithTracer m indexer)

tracer :: Lens' (WithTracer m indexer event) (Tracer m (ProcessedInput event))
tracer = tracerWrapper . wrapperConfig . unwrapTracer

tracedIndexer :: Lens' (WithTracer m indexer event) (indexer event)
tracedIndexer = tracerWrapper . wrappedIndexer

instance
    (Applicative m, IsIndex m event index) =>
    IsIndex m event (WithTracer m index) where

    index timedEvent indexer = do
        res <- indexVia tracedIndexer timedEvent indexer
        Tracer.traceWith (indexer ^. tracer) $ Index timedEvent
        pure res

instance
    ( Monad m
    , Rewindable m event index
    ) => Rewindable m event (WithTracer m index) where

    rewind p indexer = let

         rewindWrappedIndexer p' = MaybeT $ rewindVia tracedIndexer p' indexer

         traceSuccessfulRewind indexer' = do
              Tracer.traceWith (indexer' ^. tracer) (Rollback p)
              pure indexer'

        in do
        res <- runMaybeT $ rewindWrappedIndexer p
        traverse traceSuccessfulRewind res

instance (Functor m, Prunable m event indexer) =>
    Prunable m event (WithTracer m indexer) where

    prune = pruneVia tracedIndexer

    pruningPoint = pruningPointVia tracedIndexer

data DelayConfig event
    = DelayConfig
        { _configDelayCapacity :: !Word
        , _configDelayLength   :: !Word
        , _configDelayBuffer   :: !(Seq (TimedEvent event))
        }

makeLenses 'DelayConfig

-- | When indexing computation is expensive, you may want to delay it to avoid expensive rollback
-- 'WithDelay' buffers events before sending them to the underlying indexer.
-- Buffered events are sent when the buffers overflows.
--
-- An indexer wrapped in 'WithDelay' won't interact nicely with coordinator at the moment,
-- as 'WithDelay' acts as it's processing an event while it only postpones the processing.
newtype WithDelay indexer event
    = WithDelay { _delayWrapper :: IndexWrapper DelayConfig indexer event}

-- | A smart constructor for 'WithDelay'
withDelay
    :: Word -- ^ capacity
    -> indexer event
    -> WithDelay indexer event
withDelay c = WithDelay . IndexWrapper (DelayConfig c 0 Seq.empty)

makeLenses 'WithDelay

deriving via (IndexWrapper DelayConfig indexer)
    instance IsSync m event indexer => IsSync m event (WithDelay indexer)

deriving via (IndexWrapper DelayConfig indexer)
    instance Resumable m event indexer => Resumable m event (WithDelay indexer)

deriving via (IndexWrapper DelayConfig indexer)
    instance Queryable m event query indexer => Queryable m event query (WithDelay indexer)

delayedIndexer :: Lens' (WithDelay indexer event) (indexer event)
delayedIndexer = delayWrapper . wrappedIndexer

delayCapacity :: Lens' (WithDelay indexer event) Word
delayCapacity = delayWrapper . wrapperConfig . configDelayCapacity

delayLength :: Lens' (WithDelay indexer event) Word
delayLength = delayWrapper . wrapperConfig . configDelayLength

delayBuffer :: Lens' (WithDelay indexer event) (Seq (TimedEvent event))
delayBuffer = delayWrapper . wrapperConfig . configDelayBuffer

instance
    (Monad m, IsIndex m event indexer) =>
    IsIndex m event (WithDelay indexer) where

    index timedEvent indexer = let

        bufferIsFull b = (b ^. delayLength) >= (b ^. delayCapacity)

        bufferEvent = (delayLength +~ 1) . (delayBuffer %~ (timedEvent <|))

        pushAndGetOldest = \case
            Empty            -> (timedEvent, Empty)
            (buffer' :|> e') -> (e', timedEvent <| buffer')

        in do
        if not $ bufferIsFull indexer
        then pure $ bufferEvent indexer
        else do
            let b = indexer ^. delayBuffer
                (oldest, buffer') = pushAndGetOldest b
            res <- indexVia delayedIndexer oldest indexer
            pure $ res & delayBuffer .~ buffer'

instance
    ( Monad m
    , Rewindable m event indexer
    , Ord (Point event)
    ) => Rewindable m event (WithDelay indexer) where

    rewind p indexer = let

        rewindWrappedIndexer p' = MaybeT $ rewindVia delayedIndexer p' indexer

        resetBuffer = (delayLength .~ 0) . (delayBuffer .~ Seq.empty)

        (after, before) =  Seq.spanl ((> p) . view point) $ indexer ^. delayBuffer

        in if Seq.null before
           then runMaybeT $ resetBuffer <$> rewindWrappedIndexer p
           else pure . pure $ indexer
                   & delayBuffer .~ after
                   & delayLength .~ fromIntegral (Seq.length after)

-- ** Pruning control

data PruningConfig event
    = PruningConfig
        { _configSecurityParam   :: !Word
          -- ^ how far can a rollback go
        , _configPruneEvery      :: !Word
          -- ^ once we have enough events, how often do we prune
        , _configNextPruning     :: !(Seq (Point event))
          -- ^ list of pruning point
        , _configStepsBeforeNext :: !Word
          -- ^ events required before next aggregation milestones
        , _configCurrentDepth    :: !Word
          -- ^ how many events aren't pruned yet
        }

makeLenses ''PruningConfig

-- | WithPruning control when we should prune an indexer
newtype WithPruning indexer event
    = WithPruning { _pruningWrapper :: IndexWrapper PruningConfig indexer event }

withPruning
    :: Word
          -- ^ how far can a rollback go
    -> Word
          -- ^ once we have enough events, how often do we prune
    -> indexer event
    -> WithPruning indexer event
withPruning sec every
    = WithPruning
    . IndexWrapper (PruningConfig sec every Seq.empty every 0)

makeLenses ''WithPruning

deriving via (IndexWrapper PruningConfig indexer)
    instance IsSync m event indexer => IsSync m event (WithPruning indexer)

deriving via (IndexWrapper PruningConfig indexer)
    instance Queryable m event query indexer => Queryable m event query (WithPruning indexer)

prunedIndexer :: Lens' (WithPruning indexer event) (indexer event)
prunedIndexer = pruningWrapper . wrappedIndexer

securityParam :: Lens' (WithPruning indexer event) Word
securityParam = pruningWrapper . wrapperConfig . configSecurityParam

pruneEvery :: Lens' (WithPruning indexer event) Word
pruneEvery = pruningWrapper . wrapperConfig . configPruneEvery

nextPruning :: Lens' (WithPruning indexer event) (Seq (Point event))
nextPruning = pruningWrapper . wrapperConfig . configNextPruning

stepsBeforeNext :: Lens' (WithPruning indexer event) Word
stepsBeforeNext = pruningWrapper . wrapperConfig . configStepsBeforeNext

currentDepth :: Lens' (WithPruning indexer event) Word
currentDepth = pruningWrapper . wrapperConfig . configCurrentDepth

pruneAt
    :: WithPruning indexer event
    -> Maybe (Point event, WithPruning indexer event)
pruneAt indexer = let

    nextPruningDepth = indexer ^. securityParam + indexer ^. pruneEvery

    reachPruningPoint = indexer ^. currentDepth >= nextPruningDepth

    dequeueNextPruningPoint =
        case indexer ^. nextPruning of
            Empty    -> Nothing
            xs :|> p -> let
                indexer' = indexer
                    & nextPruning .~ xs
                    & currentDepth -~ indexer ^. pruneEvery
                in Just (p, indexer')

    in guard reachPruningPoint *> dequeueNextPruningPoint


startNewStep
    :: Point event
    -> WithPruning indexer event
    -> WithPruning indexer event
startNewStep p indexer
    = indexer
        & nextPruning %~ (p <|)
        & stepsBeforeNext .~ (indexer ^. pruneEvery)

tick
    :: Point event
    -> WithPruning indexer event
    -> (Maybe (Point event), WithPruning indexer event)
tick p indexer = let

    countEvent = (currentDepth +~ 1) . (stepsBeforeNext -~ 1)

    adjustStep ix = if ix ^. stepsBeforeNext == 0
        then startNewStep p ix
        else ix

    indexer' = adjustStep $ countEvent indexer

    in maybe (Nothing, indexer') (first Just) $ pruneAt indexer'


instance
    (Monad m, Ord (Point event), Prunable m event indexer, IsIndex m event indexer) =>
    IsIndex m event (WithPruning indexer) where

    index timedEvent indexer = do
        indexer' <- indexVia prunedIndexer timedEvent indexer
        let (mp, indexer'') = tick (timedEvent ^. point) indexer'
        maybe
          (pure indexer'')
          (\p -> pruneVia prunedIndexer p indexer)
          mp

-- | The rewindable instance for `WithPruning` is a defensive heuristic
-- that may provide a non optimal behaviour but ensure that we don't
-- mess up with the rollbackable events.
instance
    ( Monad m
    , Prunable m event indexer
    , Rewindable m event indexer
    , Ord (Point event)
    ) => Rewindable m event (WithPruning indexer) where

    rewind p indexer = let

        rewindWrappedIndexer
            :: Point event
            -> WithPruning indexer event
            -> MaybeT m (WithPruning indexer event)
        rewindWrappedIndexer p' = MaybeT . rewindVia prunedIndexer p'

        resetStep :: WithPruning indexer event -> WithPruning indexer event
        resetStep = do
            stepLength <- view pruneEvery
            set stepsBeforeNext stepLength

        removePruningPointsAfterRollback
            :: Point event
            -> WithPruning indexer event -> WithPruning indexer event
        removePruningPointsAfterRollback p' = nextPruning %~ Seq.dropWhileL (> p')

        countFromPruningPoints :: WithPruning indexer event -> WithPruning indexer event
        countFromPruningPoints = do
            points <- view nextPruning
            stepLength <- view pruneEvery
            -- We can safely consider that for each Pruning point still in the pipe,
            -- we have 'stepLength' events available in the indexer
            set currentDepth (fromIntegral $ length points * fromIntegral stepLength)

        isRollbackAfterPruning :: MaybeT m Bool
        isRollbackAfterPruning = MaybeT $ do
            p' <- pruningPoint $ indexer ^. prunedIndexer
            pure $ pure $ maybe True (p >=) p'

        in runMaybeT $ do
            guard =<< isRollbackAfterPruning
            countFromPruningPoints
                . removePruningPointsAfterRollback p
                . resetStep
                <$> rewindWrappedIndexer p indexer

-- ** Mixed indexer

data MixedIndexerConfig store event
    = MixedIndexerConfig
        { _configKeepInMemory :: Word
        -- ^ how many events are kept in memory after a flush
        , _configInDatabase   :: store event
        -- ^ In database storage, usually for data that can't be rollbacked
        }

makeLenses 'MixedIndexerConfig

-- | An indexer that has at most '_blocksListIndexer' events in memory and put the older one in database.
-- The query interface for this indexer will alwyas go through the database first and then prune
-- results present in memory.
--
-- @mem@ the indexer that handle old events, when we need to remove stuff from memory
-- @store@ the indexer that handle the most recent events
newtype MixedIndexer store mem event
    = MixedIndexer { _mixedWrapper :: IndexWrapper (MixedIndexerConfig store) mem event}

mixedIndexer
    :: Word
    -- ^ how many events are kept in memory after a flush
    -> store event
    -> mem event
    -> MixedIndexer store mem event
mixedIndexer keepNb db
    = MixedIndexer . IndexWrapper (MixedIndexerConfig keepNb db)

makeLenses 'MixedIndexer

keepInMemory :: Lens' (MixedIndexer store mem event) Word
keepInMemory = mixedWrapper . wrapperConfig . configKeepInMemory

inMemory :: Lens' (MixedIndexer store mem event) (mem event)
inMemory = mixedWrapper . wrappedIndexer

inDatabase :: Lens' (MixedIndexer store mem event) (store event)
inDatabase = mixedWrapper . wrapperConfig . configInDatabase

-- | Flush all the in-memory events to the database, keeping track of the latest index
flush ::
    ( Monad m
    , IsIndex m event store
    , Flushable m mem
    , Traversable (Container mem)
    , Eq (Point event)
    ) => MixedIndexer store mem event ->
    m (MixedIndexer store mem event)
flush indexer = do
    let keep = indexer ^. keepInMemory
    (eventsToFlush, indexer') <- getCompose $ inMemory (Compose . flushMemory keep) indexer
    inDatabase (indexAll eventsToFlush) indexer'

instance
    ( Monad m
    , Flushable m mem
    , Traversable (Container mem)
    , IsIndex m event mem
    , IsIndex m event store
    ) => IsIndex m event (MixedIndexer store mem) where

    index timedEvent indexer = do
        full <- isFull $ indexer ^. inMemory
        indexer' <- (if full then flush else pure) indexer
        indexVia inMemory timedEvent indexer'

instance IsSync event m mem => IsSync event m (MixedIndexer store mem) where
    lastSyncPoint = lastSyncPoint . view inMemory

instance
    ( Monad m
    , Rewindable m event store
    ) => Rewindable m event (MixedIndexer store ListIndexer) where

    rewind p indexer = let

        rewindInStore :: Rewindable m event index => index event -> MaybeT m (index event)
        rewindInStore = MaybeT . rewind p

        in runMaybeT $ do
            ix <- inMemory rewindInStore indexer
            guard $ not $ null $ ix ^. inMemory . events
            inDatabase rewindInStore ix

instance
    ( ResumableResult m event query ListIndexer
    , Queryable m event query store
    ) =>
    Queryable m event query (MixedIndexer store ListIndexer) where

    query valid q indexer
        = resumeResult valid q
            (indexer ^. inMemory)
            (query valid q (indexer ^. inDatabase))

